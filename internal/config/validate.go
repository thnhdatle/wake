// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	maxSourceNameUnkeyed  = 51 // wake_<name>_insert must fit the 63-byte identifier limit
	maxPort               = 65535
	sourceNameDescription = "[a-z0-9_]{1,58}"
	identifierDescription = "[A-Za-z0-9_-]+"
)

var (
	sourceNameRE = regexp.MustCompile(`^[a-z0-9_]{1,58}$`)
	envNameRE    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// validator accumulates every problem so they are reported at once.
type validator struct{ errs []error }

func (v *validator) add(key, format string, args ...any) {
	v.errs = append(v.errs, &ValidationError{Path: key, Message: fmt.Sprintf(format, args...)})
}

func (v *validator) result() error { return errors.Join(v.errs...) }

// Validate checks the whole configuration and reports every problem found as
// a joined list of *ValidationError (each matches ErrInvalid). The
// cache_invalidator section is checked strictly only when it is configured
// (see CacheInvalidator.Configured).
func (c Config) Validate() error {
	v := &validator{}
	c.validateSource(v)
	c.validateTables(v)
	c.validateSnapshot(v)
	c.validateStream(v)
	c.validateSinks(v)
	c.validateLag(v)
	v.requirePositive("heartbeat.interval", int64(c.Heartbeat.Interval))
	v.listen("admin.listen", c.Admin.Listen)
	c.CacheInvalidator.validate(v, c.CacheInvalidator.Configured())
	return v.result()
}

// ValidateCacheInvalidator checks the cache_invalidator section strictly, as
// `wake cache-invalidator` needs it, ignoring every other section.
func (c Config) ValidateCacheInvalidator() error {
	v := &validator{}
	c.CacheInvalidator.validate(v, true)
	return v.result()
}

func (c Config) validateSource(v *validator) {
	s := c.Source
	switch {
	case s.Name == "":
		v.add("source.name", "required; must match %s", sourceNameDescription)
	case !sourceNameRE.MatchString(s.Name):
		v.add("source.name", "%q must match %s", s.Name, sourceNameDescription)
	case s.Publication.Mode == PublicationManaged && c.Tables.EmitUnkeyed && len(s.Name) > maxSourceNameUnkeyed:
		v.add("source.name", "%q has %d characters; at most %d with publication.mode managed and "+
			"tables.emit_unkeyed", s.Name, len(s.Name), maxSourceNameUnkeyed)
	}
	v.envName("source.dsn_env", s.DSNEnv, true)
	switch s.Publication.Mode {
	case PublicationManaged:
	case PublicationExisting:
		if s.Publication.Name == "" {
			v.add("source.publication.name", "required when source.publication.mode is existing")
		}
	default:
		v.add("source.publication.mode", "%q must be managed or existing", s.Publication.Mode)
	}
}

func (c Config) validateTables(v *validator) {
	t := c.Tables
	if c.Source.Publication.Mode == PublicationManaged && len(t.Include) == 0 {
		v.add("tables.include", "at least one glob is required when source.publication.mode is managed")
	}
	v.globs("tables.include", t.Include)
	v.globs("tables.exclude", t.Exclude)
	v.requirePositive("tables.refresh_interval", int64(t.RefreshInterval))
	c.validateNameMap(v)
}

// validateNameMap checks keys, targets, collisions and the topic length of
// every mapped name for each Kafka sink.
func (c Config) validateNameMap(v *validator) {
	keys := make([]string, 0, len(c.Tables.NameMap))
	for k := range c.Tables.NameMap {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	seen := make(map[string]string, len(keys))
	for _, k := range keys {
		key := fmt.Sprintf("tables.name_map[%q]", k)
		target := c.Tables.NameMap[k]
		if _, _, ok := SplitQualified(k); !ok {
			v.add(key, "key must have the form <schema>.<table>")
		}
		schema, table, ok := SplitQualified(target)
		if !ok {
			v.add(key, "target %q must have the form <schema>.<table>", target)
			continue
		}
		if !ValidIdentifier(schema) || !ValidIdentifier(table) {
			v.add(key, "target %q: schema and table must match %s", target, identifierDescription)
			continue
		}
		if other, dup := seen[target]; dup {
			v.add(key, "target %q is also the target of %q", target, other)
		}
		seen[target] = k
		for i, s := range c.Sinks {
			if s.Type != SinkKafka || s.Kafka == nil || !ValidIdentifier(s.Kafka.TopicPrefix) {
				continue
			}
			if _, err := TopicName(s.Kafka.TopicPrefix, schema, table); err != nil {
				v.add(key, "target %q on sinks[%d] (%s): %v", target, i, s.Name, err)
			}
		}
	}
}

func (c Config) validateSnapshot(v *validator) {
	s := c.Snapshot
	if s.Mode != SnapshotInitial && s.Mode != SnapshotNever {
		v.add("snapshot.mode", "%q must be initial or never", s.Mode)
	}
	if s.ChunkRows <= 0 {
		v.add("snapshot.chunk_rows", "%d must be greater than 0", s.ChunkRows)
	}
	if s.ParallelTables < 1 {
		v.add("snapshot.parallel_tables", "%d must be at least 1", s.ParallelTables)
	}
}

func (c Config) validateStream(v *validator) {
	s := c.Stream
	v.requirePositive("stream.max_tx_bytes", int64(s.MaxTxBytes))
	v.requireNonEmpty("stream.spill_dir", s.SpillDir)
	v.requirePositive("stream.status_interval", int64(s.StatusInterval))
	if s.MinConfirmBytes < 0 {
		v.add("stream.min_confirm_bytes", "%s must not be negative", s.MinConfirmBytes)
	}
	v.requireNonEmpty("state.path", c.State.Path)
}

func (c Config) validateLag(v *validator) {
	l := c.Lag
	v.requirePositive("lag.interval", int64(l.Interval))
	v.requirePositive("lag.warn_bytes", int64(l.WarnBytes))
	v.requirePositive("lag.critical_bytes", int64(l.CriticalBytes))
	if l.WarnBytes > 0 && l.CriticalBytes > 0 && l.WarnBytes >= l.CriticalBytes {
		v.add("lag.warn_bytes", "%s must be less than lag.critical_bytes (%s)", l.WarnBytes, l.CriticalBytes)
	}
	v.requirePositive("lag.max_ack_age", int64(l.MaxAckAge))
	if l.AlertWebhook != "" {
		v.url("lag.alert_webhook", l.AlertWebhook, "http", "https")
	}
}

func (v *validator) requireNonEmpty(key, val string) {
	if strings.TrimSpace(val) == "" {
		v.add(key, "must not be empty")
	}
}

func (v *validator) requirePositive(key string, n int64) {
	if n <= 0 {
		v.add(key, "must be greater than 0")
	}
}

// envName checks that val names an environment variable.
func (v *validator) envName(key, val string, required bool) {
	switch {
	case val == "" && required:
		v.add(key, "required: the name of an environment variable")
	case val != "" && !envNameRE.MatchString(val):
		v.add(key, "%q is not an environment variable name; secrets are read from the environment, "+
			"put the variable name here, never the value", val)
	}
}

// listen checks a host:port listen address.
func (v *validator) listen(key, addr string) {
	if err := checkHostPort(addr, false); err != nil {
		v.add(key, "%q: %v", addr, err)
	}
}

// hostPort checks a dial address, which needs a host.
func (v *validator) hostPort(key, addr string) {
	if err := checkHostPort(addr, true); err != nil {
		v.add(key, "%q: %v", addr, err)
	}
}

func checkHostPort(addr string, needHost bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if needHost && host == "" {
		return errors.New("missing host")
	}
	if n, perr := strconv.Atoi(port); perr != nil || n < 0 || n > maxPort {
		return fmt.Errorf("invalid port %q", port)
	}
	return nil
}

// url checks an absolute URL with one of the schemes and no embedded password.
func (v *validator) url(key, raw string, schemes ...string) {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		v.add(key, "%q: %v", raw, err)
	case !slices.Contains(schemes, u.Scheme):
		v.add(key, "%q must use one of the schemes %s", raw, strings.Join(schemes, ", "))
	case u.Host == "":
		v.add(key, "%q has no host", raw)
	default:
		if _, has := u.User.Password(); has {
			v.add(key, "must not embed a password; secrets go in environment variables")
		}
	}
}

func (v *validator) globs(key string, globs []string) {
	for i, g := range globs {
		if _, err := path.Match(g, ""); err != nil || g == "" {
			v.add(fmt.Sprintf("%s[%d]", key, i), "%q is not a valid glob", g)
		}
	}
}

func (v *validator) tls(key string, t TLS) {
	if (t.CertFile == "") != (t.KeyFile == "") {
		v.add(key+".cert_file", "cert_file and key_file must be set together")
	}
}

func (v *validator) sasl(key string, s SASL) {
	switch s.Mechanism {
	case SASLNone:
		return
	case SASLScramSHA256, SASLScramSHA512:
		v.envName(key+".username_env", s.UsernameEnv, true)
		v.envName(key+".password_env", s.PasswordEnv, true)
	default:
		v.add(key+".mechanism", "%q must be empty, SCRAM-SHA-256 or SCRAM-SHA-512", s.Mechanism)
	}
}
