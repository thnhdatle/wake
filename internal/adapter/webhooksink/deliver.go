// SPDX-License-Identifier: Apache-2.0

package webhooksink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/thnhdatle/wake/internal/domain/pipeline"
)

const (
	userAgent = "wake-webhook/1"
	// maxDrain bounds the response body read to reuse the connection.
	maxDrain = 64 << 10
	// cappedAttempt makes Backoff.Delay return its capped delay.
	cappedAttempt = 64
)

type outcome uint8

const (
	outcomeOK outcome = iota
	outcomeRetry
	outcomePoison
)

// result is the classified outcome of one POST attempt.
type result struct {
	outcome    outcome
	reason     string // RetryReason* for outcomeRetry and outcomePoison
	status     int    // HTTP status, 0 without a response
	retryAfter time.Duration
	err        error
}

// attempt POSTs the batch once. The delivery id is created on the first
// attempt and reused by every retry; the signature is computed for each
// attempt with the current time.
func (s *Sink) attempt(ctx context.Context, d *delivery) result {
	if d.id == "" {
		id, err := s.newDeliveryID()
		if err != nil {
			return result{outcome: outcomeRetry, reason: RetryReasonInternal, err: err}
		}
		d.id = id
	}
	start := s.clock.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(d.body))
	if err != nil {
		return result{outcome: outcomeRetry, reason: RetryReasonInternal, err: fmt.Errorf("webhooksink %s: request: %w",
			s.name, stripURL(err))}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set(DeliveryIDHeader, d.id)
	req.Header.Set(SignatureHeader, Sign(s.secret, start.Unix(), d.body))
	s.obs.ObserveSent(s.name, len(d.batch.Records), len(d.body))
	resp, err := s.client.Do(req)
	s.obs.ObserveLatency(s.name, s.clock.Now().Sub(start))
	if err != nil {
		return result{outcome: outcomeRetry, reason: RetryReasonNetwork, err: fmt.Errorf("webhooksink %s: post: %w",
			s.name, stripURL(err))}
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrain))
	return classify(resp)
}

// stripURL drops the *url.Error wrapper, whose message repeats the URL and
// so possibly a token in its query.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

func classify(resp *http.Response) result {
	code := resp.StatusCode
	res := result{status: code, err: &StatusError{StatusCode: code}}
	switch {
	case code >= 200 && code < 300:
		return result{outcome: outcomeOK, status: code}
	case code == http.StatusRequestTimeout:
		res.outcome, res.reason = outcomeRetry, RetryReasonTimeout
	case code == http.StatusTooManyRequests:
		res.outcome, res.reason = outcomeRetry, RetryReasonRateLimited
		res.retryAfter = retryAfter(resp.Header.Get("Retry-After"))
	case code >= http.StatusInternalServerError:
		res.outcome, res.reason = outcomeRetry, RetryReasonServerError
		res.retryAfter = retryAfter(resp.Header.Get("Retry-After"))
	default:
		res.outcome, res.reason = outcomePoison, RetryReasonPoison
	}
	return res
}

// retryAfter parses a Retry-After header in delay-seconds form; the
// HTTP-date form is ignored.
func retryAfter(v string) time.Duration {
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

// handle acts on an attempt's result: it acknowledges, schedules a retry or
// dead-letters, and returns the wait before the next attempt or done.
func (s *Sink) handle(ctx context.Context, d *delivery, res *result, attempt int) (time.Duration, bool) {
	switch res.outcome {
	case outcomeOK:
		s.ack(ctx, d, true)
		s.setHealth(d, pipeline.SinkStateHealthy, "", "")
		return 0, true
	case outcomeRetry:
		s.obs.ObserveRetry(s.name, res.reason)
		s.setHealth(d, pipeline.SinkStateRetrying, "", res.err.Error())
		s.logAttempt(ctx, d, res, attempt, slog.LevelWarn, "delivery failed, retrying")
		ceiling := s.backoff.Delay(cappedAttempt, 1)
		return max(s.backoff.Delay(attempt, s.jitter()), min(res.retryAfter, ceiling)), false
	case outcomePoison:
	}
	return s.handlePoison(ctx, d, res, attempt)
}

// handlePoison retries a poison batch up to max_attempts, then dead-letters
// and acknowledges it, or, without a dead-letter directory, keeps retrying
// at the capped backoff with Health sink_fatal.
func (s *Sink) handlePoison(ctx context.Context, d *delivery, res *result, attempt int) (time.Duration, bool) {
	d.poison++
	if d.poison < s.maxAttempts {
		s.obs.ObserveRetry(s.name, RetryReasonPoison)
		s.setHealth(d, pipeline.SinkStateRetrying, "", res.err.Error())
		s.logAttempt(ctx, d, res, attempt, slog.LevelWarn, "receiver rejected batch, retrying")
		return s.backoff.Delay(attempt, s.jitter()), false
	}
	capped := s.backoff.Delay(cappedAttempt, s.jitter())
	if s.deadLetterDir == "" {
		s.obs.ObserveRetry(s.name, RetryReasonPoison)
		s.setHealth(d, pipeline.SinkStateFatal, pipeline.CodeSinkFatal, fmt.Sprintf(
			"receiver rejects batch %s with HTTP %d after %d attempts and webhook.dead_letter_dir is empty",
			d.id, res.status, d.poison))
		s.logBlocked(ctx, d, res, attempt, "poison batch blocks the sink until the receiver accepts it")
		return capped, false
	}
	if err := writeDeadLetter(s.deadLetterDir, d.id, d.batch.Records); err != nil {
		s.obs.ObserveRetry(s.name, RetryReasonDeadLetter)
		s.setHealth(d, pipeline.SinkStateFatal, pipeline.CodeSinkFatal, err.Error())
		s.logBlocked(ctx, d, &result{status: res.status, err: err}, attempt, "dead-letter write failed, retrying")
		return capped, false
	}
	s.ack(ctx, d, false)
	s.obs.ObserveDeadLettered(s.name, len(d.batch.Records))
	s.setHealth(d, pipeline.SinkStateHealthy, "", "")
	s.logAttempt(ctx, d, res, attempt, slog.LevelError,
		"poison batch written to the dead-letter directory and acknowledged")
	return 0, true
}

// setHealth records the state of the batch's lane.
func (s *Sink) setHealth(d *delivery, st pipeline.SinkState, code, reason string) {
	s.mu.Lock()
	s.health.set(d.batch.Lane, st, code, reason, s.clock.Now())
	s.mu.Unlock()
}

// logBlocked logs a blocking failure at error level once per batch, then at
// debug level.
func (s *Sink) logBlocked(ctx context.Context, d *delivery, res *result, attempt int, msg string) {
	level := slog.LevelDebug
	if !d.blocked {
		d.blocked, level = true, slog.LevelError
	}
	s.logAttempt(ctx, d, res, attempt, level, msg)
}

// logAttempt logs one attempt; retries after the first are logged at debug
// level to keep a long outage from flooding the log.
func (s *Sink) logAttempt(ctx context.Context, d *delivery, res *result, attempt int, level slog.Level, msg string) {
	if level == slog.LevelWarn && attempt > 0 {
		level = slog.LevelDebug
	}
	s.log.LogAttrs(ctx, level, msg, slog.String("operation", "deliver"), slog.String("request_id", d.id),
		slog.Int("attempt", attempt+1), slog.Int("status", res.status),
		slog.Int("count", len(d.batch.Records)), slog.Int("bytes", len(d.body)), slog.Any("error", res.err))
}
