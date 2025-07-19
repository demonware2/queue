-- Rate limiting Lua script for Redis

local maxTokens = 10
local refillRate = 5
local keyExpiry = 300
local key = "api_BMN_limit"

local currentTimeRedis = redis.call("TIME")
local currentTime = currentTimeRedis[1] 

local currentTokens = tonumber(redis.call("HGET", key, "tokens") or "0")
local lastRequest = tonumber(redis.call("HGET", key, "last_request"))

if not lastRequest then
    redis.log(redis.LOG_NOTICE, "Initializing tokens for key: " .. key)
    redis.call("HSET", key, "last_request", currentTime)
    redis.call("HSET", key, "tokens", maxTokens - 1)
    redis.call("EXPIRE", key, keyExpiry)
    return 1
else
    local elapsedSeconds = currentTime - lastRequest
    local newTokens = math.min(maxTokens, currentTokens + elapsedSeconds * refillRate)
    
    if newTokens > 0 then
        redis.call("HSET", key, "tokens", newTokens - 1)
        redis.call("HSET", key, "last_request", currentTime)
        return 1
    else
        redis.log(redis.LOG_WARNING, "Rate limit hit for key: " .. key)
        return 0
    end
end
