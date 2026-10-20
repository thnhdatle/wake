// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"net/url"
	"strings"
)

func (c Config) validateSinks(v *validator) {
	if len(c.Sinks) == 0 {
		v.add("sinks", "at least one sink is required")
		return
	}
	names := make(map[string]int, len(c.Sinks))
	for i := range c.Sinks {
		s := c.Sinks[i]
		key := fmt.Sprintf("sinks[%d]", i)
		if s.Name == "" {
			v.add(key+".name", "must not be empty")
		} else if first, dup := names[s.Name]; dup {
			v.add(key+".name", "%q duplicates sinks[%d].name", s.Name, first)
		} else {
			names[s.Name] = i
		}
		s.validate(v, key)
	}
}

func (s Sink) validate(v *validator, key string) {
	v.globs(key+".tables", s.Tables)
	if s.MaxEvents < 1 {
		v.add(key+".max_events", "%d must be at least 1", s.MaxEvents)
	}
	v.requirePositive(key+".max_bytes", int64(s.MaxBytes))
	if s.Linger < 0 {
		v.add(key+".linger", "must not be negative")
	}
	if s.MaxEventBytes < 0 {
		v.add(key+".max_event_bytes", "must not be negative")
	}
	switch s.Type {
	case SinkKafka:
		s.Kafka.validate(v, key+".kafka")
	case SinkNATS:
		s.NATS.validate(v, key+".nats")
	case SinkWebhook:
		s.Webhook.validate(v, key+".webhook")
	case SinkStdout:
	default:
		v.add(key+".type", "%q must be kafka, nats, webhook or stdout", s.Type)
		return
	}
	s.rejectForeignSections(v, key)
}

// rejectForeignSections flags a sub-section that does not belong to the type.
func (s Sink) rejectForeignSections(v *validator, key string) {
	for _, sec := range []struct {
		name    string
		typ     SinkType
		present bool
	}{
		{"kafka", SinkKafka, s.Kafka != nil},
		{"nats", SinkNATS, s.NATS != nil},
		{"webhook", SinkWebhook, s.Webhook != nil},
	} {
		if sec.present && s.Type != sec.typ {
			v.add(key+"."+sec.name, "not allowed for a sink of type %s", s.Type)
		}
	}
}

func (k *KafkaSink) validate(v *validator, key string) {
	if k == nil {
		v.add(key, "section required for a sink of type kafka")
		return
	}
	validateBrokers(v, key+".brokers", k.Brokers)
	validatePrefix(v, key+".topic_prefix", k.TopicPrefix)
	if k.Partitions < brokerDefault || k.Partitions == 0 {
		v.add(key+".partitions", "%d must be -1 (broker default) or at least 1", k.Partitions)
	}
	if k.ReplicationFactor < brokerDefault || k.ReplicationFactor == 0 {
		v.add(key+".replication_factor", "%d must be -1 (broker default) or at least 1", k.ReplicationFactor)
	}
	v.tls(key+".tls", k.TLS)
	v.sasl(key+".sasl", k.SASL)
}

func (n *NATSSink) validate(v *validator, key string) {
	if n == nil {
		v.add(key, "section required for a sink of type nats")
		return
	}
	validateNATSURL(v, key+".url", n.URL)
	validatePrefix(v, key+".subject_prefix", n.SubjectPrefix)
	if n.CreateStream {
		v.requirePositive(key+".duplicate_window", int64(n.DuplicateWindow))
	}
	v.tls(key+".tls", n.TLS)
}

func (w *WebhookSink) validate(v *validator, key string) {
	if w == nil {
		v.add(key, "section required for a sink of type webhook")
		return
	}
	if w.URL == "" {
		v.add(key+".url", "required")
	} else {
		v.url(key+".url", w.URL, "http", "https")
	}
	v.envName(key+".secret_env", w.SecretEnv, true)
	v.requirePositive(key+".timeout", int64(w.Timeout))
	if w.Lanes < 1 {
		v.add(key+".lanes", "%d must be at least 1", w.Lanes)
	}
	if w.MaxAttempts < 1 {
		v.add(key+".max_attempts", "%d must be at least 1", w.MaxAttempts)
	}
}

func validateBrokers(v *validator, key string, brokers []string) {
	if len(brokers) == 0 {
		v.add(key, "at least one host:port is required")
	}
	for i, b := range brokers {
		v.hostPort(fmt.Sprintf("%s[%d]", key, i), b)
	}
}

func validatePrefix(v *validator, key, prefix string) {
	switch {
	case prefix == "":
		v.add(key, "required; must match %s", identifierDescription)
	case !ValidIdentifier(prefix):
		v.add(key, "%q must match %s", prefix, identifierDescription)
	}
}

// validateNATSURL accepts one URL or a comma-separated list.
func validateNATSURL(v *validator, key, raw string) {
	if strings.TrimSpace(raw) == "" {
		v.add(key, "required")
		return
	}
	for part := range strings.SplitSeq(raw, ",") {
		u, err := url.Parse(strings.TrimSpace(part))
		switch {
		case err != nil:
			v.add(key, "%q: %v", part, err)
		case u.Scheme != "nats" && u.Scheme != "tls" && u.Scheme != "ws" && u.Scheme != "wss":
			v.add(key, "%q must use the scheme nats, tls, ws or wss", part)
		case u.Host == "":
			v.add(key, "%q has no host", part)
		default:
			if _, has := u.User.Password(); has {
				v.add(key, "must not embed a password; use nats.credentials_file")
			}
		}
	}
}
