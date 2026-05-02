package repository

import "github.com/redis/go-redis/v9"

var joinRoomScript = redis.NewScript(`
local position = redis.call("GET", KEYS[1])
if position then
	return position
end

position = redis.call("INCR", KEYS[2])
redis.call("SET", KEYS[1], position)
return position
`)

var issueAdmissionTokenScript = redis.NewScript(`
local existingToken = redis.call("GET", KEYS[3])
if existingToken then
	local ttl = redis.call("TTL", KEYS[3])
	if ttl < 0 then
		ttl = 0
	end
	local existingLease = redis.call("GET", KEYS[6]) or ""
	return {3, existingToken, ttl, existingLease}
end

local position = redis.call("GET", KEYS[1])
if not position then
	return {0, "", 0, ""}
end

position = tonumber(position)
if not position then
	return redis.error_reply("invalid session position")
end

local admitted = tonumber(redis.call("GET", KEYS[2]) or "0")
if not admitted then
	return redis.error_reply("invalid admitted counter")
end

if position > admitted then
	return {1, "", 0, ""}
end

local now = tonumber(ARGV[3])
if not now then
	return redis.error_reply("invalid current time")
end
local maxActive = tonumber(ARGV[7])
if not maxActive then
	return redis.error_reply("invalid max active admissions")
end

local offerScore = redis.call("ZSCORE", KEYS[4], tostring(position))
local hasOffer = 0
if offerScore then
	offerScore = tonumber(offerScore)
	if not offerScore then
		return redis.error_reply("invalid admission offer score")
	end
	if offerScore > now then
		hasOffer = 1
	else
		redis.call("ZREM", KEYS[4], tostring(position))
	end
end

redis.call("ZREMRANGEBYSCORE", KEYS[5], "-inf", now)
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", now)

if hasOffer == 0 then
	local active = redis.call("ZCARD", KEYS[5])
	local offers = redis.call("ZCARD", KEYS[4])
	local available = maxActive - active - offers
	if available <= 0 then
		return {4, "", 0, ""}
	end
end

redis.call("SET", KEYS[3], ARGV[1], "EX", ARGV[2])
redis.call("ZREM", KEYS[4], tostring(position))
redis.call("ZADD", KEYS[5], ARGV[4], ARGV[5])
redis.call("SET", KEYS[6], ARGV[5], "EX", ARGV[2])
redis.call("SET", KEYS[7], ARGV[6], "EX", ARGV[2])
redis.call("DEL", KEYS[1])
return {2, ARGV[1], tonumber(ARGV[2]), ARGV[5]}
`)

var advanceAdmissionScript = redis.NewScript(`
local arrived = tonumber(redis.call("GET", KEYS[1]) or "0")
if not arrived then
	return redis.error_reply("invalid arrival counter")
end

local admitted = tonumber(redis.call("GET", KEYS[2]) or "0")
if not admitted then
	return redis.error_reply("invalid admitted counter")
end

local now = tonumber(ARGV[2])
if not now then
	return redis.error_reply("invalid current time")
end
local offerExpiresAt = tonumber(ARGV[3])
if not offerExpiresAt then
	return redis.error_reply("invalid offer expiry")
end
local maxActive = tonumber(ARGV[4])
if not maxActive then
	return redis.error_reply("invalid max active admissions")
end
if maxActive < 0 then
	return redis.error_reply("negative max active admissions")
end

local increment = tonumber(ARGV[1])
if not increment then
	return redis.error_reply("invalid admission increment")
end
if increment < 0 then
	return redis.error_reply("negative admission increment")
end

redis.call("ZREMRANGEBYSCORE", KEYS[3], "-inf", now)
redis.call("ZREMRANGEBYSCORE", KEYS[4], "-inf", now)

local active = redis.call("ZCARD", KEYS[3])
local offers = redis.call("ZCARD", KEYS[4])
local available = maxActive - active - offers
if available < 0 then
	available = 0
end

local queued = arrived - admitted
if queued < 0 then
	queued = 0
end

local admit = increment
if admit > available then
	admit = available
end
if admit > queued then
	admit = queued
end

local target = admitted + admit
if target > admitted then
	for arrival = admitted + 1, target do
		redis.call("ZADD", KEYS[4], offerExpiresAt, tostring(arrival))
	end
	redis.call("SET", KEYS[2], target)
end

active = redis.call("ZCARD", KEYS[3])
offers = redis.call("ZCARD", KEYS[4])
return {arrived, admitted, target, active, offers, maxActive}
`)

var releaseAdmissionScript = redis.NewScript(`
local lease = redis.call("GET", KEYS[3])
if lease and lease ~= ARGV[1] then
	return {-1}
end
local session = redis.call("GET", KEYS[4])
if session and session ~= ARGV[2] then
	return {-1}
end

local removed = redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("DEL", KEYS[2])
redis.call("DEL", KEYS[3])
redis.call("DEL", KEYS[4])

return {removed}
`)

var acquireWorkerLockScript = redis.NewScript(`
local owner = redis.call("GET", KEYS[1])
if not owner then
    local ok = redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")
    if ok then
        return 1
    end
    return 0
end

if owner == ARGV[1] then
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	return 1
end

return 0
`)
