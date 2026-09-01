-- 单维度限流脚本
-- 基于滑动时间窗口的单维度原子限流
-- 由切面逐条调用实现多维度限流

-- 参数说明：
-- KEYS[1]: 限流维度键
-- ARGV[1]: 当前时间戳（毫秒）
-- ARGV[2]: 申请令牌数
-- ARGV[3]: 时间窗口（毫秒）
-- ARGV[4]: 最大令牌数（窗口内允许的总数）
-- ARGV[5]: 请求唯一标识



-- 用一个具体例子来理解
-- 场景设定： 某个 API 接口，限制每个用户每秒最多调用 3 次。
-- key        = "user:张三:api:/order"
-- interval   = 1000ms（1秒窗口）
-- max_tokens = 3（窗口内最多3次）
-- permits    = 1（每次请求消耗1个令牌）

-- Redis 里实际存了什么
-- 两个 key：
-- "user:张三:api:/order:value"    → String，存当前剩余令牌数
-- "user:张三:api:/order:permits"  → Sorted Set，存每次消耗的记录
-- Sorted Set 的结构：
-- member（成员）              score（分数）
-- ─────────────────────────  ─────────────
-- "req-uuid-001:1"           1718000000100   ← 第1次请求的时间戳
-- "req-uuid-002:1"           1718000000400   ← 第2次请求的时间戳
-- "req-uuid-003:1"           1718000000700   ← 第3次请求的时间戳

-- score 就是时间戳，Redis 按 score 排序，所以"最早的请求"排在最前面，方便捞出过期的。


-- 时间轴演示
-- T=100ms，张三第1次请求
-- 脚本入参: now_ms=100, permits=1, interval=1000, max_tokens=3

-- ① GET value_key → nil（首次，不存在）
--      current_val = max_tokens = 3

-- ② ZRANGEBYSCORE permits_key 0 (100-1000)=-900
--   → 没有 score ≤ -900 的记录，过期列表为空，跳过回收

-- ③ current_val(3) >= permits(1)，通过 ✅

-- ④ ZADD permits_key  score=100  member="req-001:1"
---  SET  value_key   2          （3-1=2）
-- 此时 Redis 状态：
-- value_key   → "2"
-- permits_key → { "req-001:1" : 100 }

-- T=400ms，张三第2次请求
-- ① GET value_key → "2"，current_val = 2

-- ② ZRANGEBYSCORE 0 到 (400-1000)=-600 → 空，跳过

-- ③ current_val(2) >= permits(1)，通过 ✅

-- ④ ZADD score=400  member="req-002:1"
--    SET  value_key  1
-- Redis 状态：
-- value_key   → "1"
-- permits_key → { "req-001:1":100,  "req-002:1":400 }

-- T=700ms，张三第3次请求
-- ① current_val = 1
-- ② 无过期，跳过
-- ③ 1 >= 1，通过 ✅
-- ④ ZADD score=700  SET value_key 0
-- Redis 状态：
-- value_key   → "0"
-- permits_key → { "req-001:1":100,  "req-002:1":400,  "req-003:1":700 }

-- T=800ms，张三第4次请求（令牌耗尽）
-- ① current_val = 0

-- ② ZRANGEBYSCORE 0 到 (800-1000)=-200 → 空，无过期

-- ③ current_val(0) < permits(1)  ← 不够！

-- return 0  ❌ 被限流

-- T=1200ms，张三第5次请求（有令牌过期归还了）
-- ① current_val = 0

-- ② ZRANGEBYSCORE 0 到 (1200-1000)=200
--    → 捞出 score ≤ 200 的记录：["req-001:1"]

--    解析 "req-001:1" 末尾的数字 → p = 1
--   expired_count = 1

--  ZREMRANGEBYSCORE 删掉这条记录
--   current_val = min(3, 0+1) = 1   ← 归还了1个令牌

-- ③ current_val(1) >= permits(1)，通过 ✅

-- ④ ZADD score=1200  member="req-005:1"
--    SET  value_key  0
-- Redis 状态：
-- value_key   → "0"
-- permits_key → { "req-002:1":400,  "req-003:1":700,  "req-005:1":1200 }
--              （req-001 已被清除，它滑出了1秒窗口）




local key = KEYS[1]
local now_ms = tonumber(ARGV[1])   -- 当前毫秒时间戳，如 1718000000000
local permits = tonumber(ARGV[2])  -- 本次申请消耗几个令牌，如 1
local interval = tonumber(ARGV[3]) -- 窗口大小（毫秒），如 1000 = 1秒
local max_tokens = tonumber(ARGV[4])-- 窗口内最多允许多少令牌，如 100
local request_id = ARGV[5]         -- 请求唯一ID，用于记录进 ZSet

-- .. 是 Lua 的字符串拼接符，相当于 Java 的 +。
local value_key = key .. ":value"    -- "user:张三:api:/order:value"    → String，存当前剩余令牌数
local permits_key = key .. ":permits" -- "user:张三:api:/order:permits"  → Sorted Set，存每次消耗的记录

-- 获取当前可用令牌（不存在则使用 max_tokens）
local current_val = tonumber(redis.call("get", value_key)) or max_tokens

-- 回收过期令牌
local expired_values = redis.call("zrangebyscore", permits_key, 0, now_ms - interval)
if #expired_values > 0 then
    local expired_count = 0
    for _, v in ipairs(expired_values) do
        local p = tonumber(string.match(v, ":(%d+)$")) -- 匹配到 每次消耗的 permits ，也即每次消耗的令牌数
        if p then
            expired_count = expired_count + p
        end
    end

    -- ZREMRANGEBYSCORE key min max — 删除 Sorted Set 中 score 在范围内的成员
    redis.call("zremrangebyscore", permits_key, 0, now_ms - interval)

    if expired_count > 0 then
        current_val = math.min(max_tokens, current_val + expired_count)
    end
end

-- 检查可用令牌
if current_val < permits then
    return 0
end

-- 扣减令牌
local permit_record = request_id .. ":" .. permits
redis.call("zadd", permits_key, now_ms, permit_record) -- 存入 Sorted Set
redis.call("set", value_key, current_val - permits) -- 存入 String 类型

-- 设置过期时间（窗口的2倍，至少1秒） ceil 函数，向上取整函数
local expire_time = math.ceil(interval * 2 / 1000)
if expire_time < 1 then expire_time = 1 end
redis.call("expire", value_key, expire_time)
redis.call("expire", permits_key, expire_time)

return 1


