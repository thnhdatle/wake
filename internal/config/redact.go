// SPDX-License-Identifier: Apache-2.0

package config

import (
	"net/url"
	"strings"
)

const redactedPassword = "xxxxx"

// Redacted returns a copy that is safe to print.
func (c Config) Redacted() Config {
	out := c
	out.Lag.AlertWebhook = redactURL(c.Lag.AlertWebhook)
	out.Sinks = make([]Sink, len(c.Sinks))
	for i, s := range c.Sinks {
		if s.Webhook != nil {
			w := *s.Webhook
			w.URL = redactURL(w.URL)
			s.Webhook = &w
		}
		if s.NATS != nil {
			n := *s.NATS
			n.URL = redactURL(n.URL)
			s.NATS = &n
		}
		out.Sinks[i] = s
	}
	out.CacheInvalidator.NATS.URL = redactURL(c.CacheInvalidator.NATS.URL)
	return out
}

func redactURL(raw string) string {
	if raw == "" {
		return raw
	}
	parts := strings.Split(raw, ",")
	for i, p := range parts {
		u, err := url.Parse(strings.TrimSpace(p))
		if err != nil || u.User == nil {
			continue
		}
		if _, ok := u.User.Password(); ok {
			u.User = url.UserPassword(u.User.Username(), redactedPassword)
			parts[i] = u.String()
		}
	}
	return strings.Join(parts, ",")
}
