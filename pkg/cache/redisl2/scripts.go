// SPDX-License-Identifier: Apache-2.0

package redisl2

import "github.com/redis/go-redis/v9"

const prelude = `
local t = redis.call('TIME')
local now_us = tonumber(t[1]) * 1000000 + tonumber(t[2])
local now_ms = math.floor(now_us / 1000)
local function fmt(n) return string.format('%d', n) end
local function gen(k, ttl)
  local g = redis.call('GET', k)
  if g and string.match(g, '^%d+$') then
    local p = redis.call('PTTL', k)
    if p >= 0 and p < ttl then redis.call('PEXPIRE', k, ttl) end
    return g
  end
  g = fmt(now_us)
  if ttl > 0 then redis.call('SET', k, g, 'PX', ttl) else redis.call('SET', k, g) end
  return g
end
local function put(k, g, payload, ttl)
  local rec = g .. '|' .. fmt(now_ms + ttl) .. '|' .. payload
  if ttl > 0 then redis.call('SET', k, rec, 'PX', ttl) else redis.call('SET', k, rec) end
end
local function epoch(k)
  local e = redis.call('GET', k)
  if e and string.match(e, '^%d+$') then return e end
  e = fmt(now_us)
  redis.call('SET', k, e)
  return e
end
`

const readScript = prelude + `
local g = gen(KEYS[2], tonumber(ARGV[1]))
local raw = redis.call('GET', KEYS[1])
if not raw then return {g} end
local i = string.find(raw, '|', 1, true)
if not i then return {g} end
local j = string.find(raw, '|', i + 1, true)
if not j or string.sub(raw, 1, i - 1) ~= g then return {g} end
local exp = string.sub(raw, i + 1, j - 1)
if not string.match(exp, '^%-?%d+$') then return {g} end
exp = tonumber(exp)
if exp <= now_ms then return {g} end
return {g, fmt(exp - now_ms), string.sub(raw, j + 1)}
`

const fillScript = prelude + `
local stored = 0
if gen(KEYS[2], tonumber(ARGV[4])) == ARGV[1] then
  put(KEYS[1], ARGV[1], ARGV[2], tonumber(ARGV[3]))
  stored = 1
end
local released = false
if ARGV[5] ~= '' and redis.call('GET', KEYS[3]) == ARGV[5] then
  redis.call('DEL', KEYS[3])
  released = true
end
if stored == 1 or released then redis.call('PUBLISH', ARGV[6], ARGV[7]) end
return stored
`

const setScript = prelude + `
gen(KEYS[2], tonumber(ARGV[3]))
local g = fmt(redis.call('INCR', KEYS[2]))
put(KEYS[1], g, ARGV[1], tonumber(ARGV[2]))
redis.call('PUBLISH', ARGV[4], ARGV[5] .. g .. '|' .. ARGV[6])
return g
`

const invalidateScript = prelude + `
gen(KEYS[2], tonumber(ARGV[1]))
local g = fmt(redis.call('INCR', KEYS[2]))
redis.call('DEL', KEYS[1])
redis.call('PUBLISH', ARGV[2], ARGV[3] .. g .. '|' .. ARGV[4])
return g
`

const unlockScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  redis.call('PUBLISH', ARGV[2], ARGV[3])
  return 1
end
return 0
`

const epochScript = prelude + `
return epoch(KEYS[1])
`

const bumpEpochScript = prelude + `
epoch(KEYS[1])
local e = fmt(redis.call('INCR', KEYS[1]))
redis.call('PUBLISH', ARGV[1], 'e|' .. e)
return e
`

type scripts struct {
	read, fill, set, invalidate, unlock, epoch, bumpEpoch *redis.Script
}

func newScripts() scripts {
	return scripts{
		read:       redis.NewScript(readScript),
		fill:       redis.NewScript(fillScript),
		set:        redis.NewScript(setScript),
		invalidate: redis.NewScript(invalidateScript),
		unlock:     redis.NewScript(unlockScript),
		epoch:      redis.NewScript(epochScript),
		bumpEpoch:  redis.NewScript(bumpEpochScript),
	}
}
