// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const cacheKey = "cache_invalidator"

var (
	namespaceRE   = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	placeholderRE = regexp.MustCompile(`\{[^{}]+\}`)
)

func (ci CacheInvalidator) validate(v *validator, strict bool) {
	v.listen(cacheKey+".admin_listen", ci.AdminListen)
	v.envName(cacheKey+".validate_dsn_env", ci.ValidateDSNEnv, false)
	v.envName(cacheKey+".redis.password_env", ci.Redis.PasswordEnv, false)
	v.tls(cacheKey+".redis.tls", ci.Redis.TLS)
	for i, a := range ci.Redis.Addrs {
		v.hostPort(fmt.Sprintf("%s.redis.addrs[%d]", cacheKey, i), a)
	}
	switch ci.Source {
	case SourceKafka:
		ci.Kafka.validate(v, strict)
	case SourceNATS:
		ci.NATS.validate(v, strict)
	default:
		v.add(cacheKey+".source", "%q must be kafka or nats", ci.Source)
	}
	if ci.Namespace != "" && !namespaceRE.MatchString(ci.Namespace) {
		v.add(cacheKey+".namespace", "%q must match %s", ci.Namespace, identifierDescription)
	}
	ci.validateTemplates(v)
	if !strict {
		return
	}
	if ci.Namespace == "" {
		v.add(cacheKey+".namespace", "required; must match %s", identifierDescription)
	}
	if ci.BusChannel == "" && ci.Namespace != "" {
		v.add(cacheKey+".bus_channel", "must not be empty")
	}
	if len(ci.Redis.Addrs) == 0 {
		v.add(cacheKey+".redis.addrs", "at least one host:port is required")
	}
	if len(ci.Templates) == 0 {
		v.add(cacheKey+".templates", "at least one <schema>.<table> template list is required")
	}
}

func (k InvalidatorKafka) validate(v *validator, strict bool) {
	key := cacheKey + ".kafka"
	if strict || len(k.Brokers) > 0 {
		validateBrokers(v, key+".brokers", k.Brokers)
	}
	if strict || k.TopicPrefix != "" {
		validatePrefix(v, key+".topic_prefix", k.TopicPrefix)
	}
	if strict {
		v.requireNonEmpty(key+".group", k.Group)
	}
	v.tls(key+".tls", k.TLS)
	v.sasl(key+".sasl", k.SASL)
}

func (n InvalidatorNATS) validate(v *validator, strict bool) {
	key := cacheKey + ".nats"
	if strict || n.URL != "" {
		validateNATSURL(v, key+".url", n.URL)
	}
	if strict || n.SubjectPrefix != "" {
		validatePrefix(v, key+".subject_prefix", n.SubjectPrefix)
	}
	if strict {
		v.requireNonEmpty(key+".durable", n.Durable)
		if n.Stream == "" {
			v.add(key+".stream", "required (default wake_<source.name>)")
		}
	}
	v.tls(key+".tls", n.TLS)
}

func (ci CacheInvalidator) validateTemplates(v *validator) {
	tables := make([]string, 0, len(ci.Templates))
	for t := range ci.Templates {
		tables = append(tables, t)
	}
	slices.Sort(tables)
	for _, t := range tables {
		key := fmt.Sprintf("%s.templates[%q]", cacheKey, t)
		if _, _, ok := SplitQualified(t); !ok {
			v.add(key, "key must have the form <schema>.<table>")
		}
		list := ci.Templates[t]
		if len(list) == 0 {
			v.add(key, "at least one key template is required")
		}
		for i, tpl := range list {
			rest := placeholderRE.ReplaceAllString(tpl, "")
			switch {
			case strings.TrimSpace(tpl) == "":
				v.add(fmt.Sprintf("%s[%d]", key, i), "template must not be empty")
			case strings.ContainsAny(rest, "{}"):
				v.add(fmt.Sprintf("%s[%d]", key, i), "%q has an unbalanced or empty {placeholder}", tpl)
			}
		}
	}
}
