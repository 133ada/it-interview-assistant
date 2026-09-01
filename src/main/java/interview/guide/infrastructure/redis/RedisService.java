package interview.guide.infrastructure.redis;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.KeysScanOptions;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Redis 服务封装
 * 提供通用的 Redis 操作，包括缓存、分布式锁、Stream 消息队列等
 *
 * 【前置知识】Redisson 是 Redis 的 Java 客户端，它把原始 Redis 命令封装成面向对象的 API。
 *             本类所有方法注释中会同步标注对应的原始 Redis 命令，方便对照理解。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisService {

    // RedissonClient 是 Redisson 的核心入口，相当于拿到了 Redis 的"连接句柄"
    // 通过它获取各种数据结构对象（RBucket / RMap / RStream 等），再调用对应操作
    private final RedissonClient redissonClient;

    // ==================== 基础键值操作（String 类型） ====================

    /**
     * 设置值（无过期时间）
     *
     * 等价 Redis 命令：SET key value
     * 示例：SET user:name "alice"
     */
    public <T> void set(String key, T value) {
        // getBucket(key)：获取该 key 对应的"桶"对象，底层对应 Redis String 类型
        // Redisson 会把 Java 对象序列化后存入 Redis
        RBucket<T> bucket = redissonClient.getBucket(key);

        // bucket.set(value)：向 Redis 写入值，无过期时间
        // 等价命令：SET key value
        bucket.set(value);
    }

    /**
     * 设置值（带过期时间）
     *
     * 等价 Redis 命令：SET key value PX milliseconds
     * 示例：SET user:token "abc123" PX 3600000   （1小时后过期）
     *
     * 说明：ttl 是 Duration 类型，Redisson 内部会自动换算成毫秒传给 Redis
     */
    public <T> void set(String key, T value, Duration ttl) {
        RBucket<T> bucket = redissonClient.getBucket(key);

        // bucket.set(value, ttl)：写入值并设置过期时间
        // 等价命令：SET key value PX {ttl的毫秒数}
        //           或：SET key value EX {ttl的秒数}
        bucket.set(value, ttl);
    }

    /**
     * 获取值
     *
     * 等价 Redis 命令：GET key
     * 示例：GET user:name  → "alice"（不存在则返回 nil，Java 侧返回 null）
     */
    public <T> T get(String key) {
        RBucket<T> bucket = redissonClient.getBucket(key);

        // bucket.get()：从 Redis 读取值，Redisson 自动反序列化成泛型 T
        // 等价命令：GET key
        return bucket.get();
    }

    /**
     * 获取值，如果不存在则使用 loader 加载并缓存（Cache-Aside 模式）
     *
     * 这是一个组合操作，内部等价于：
     *   ① GET key
     *   ② 若结果为 nil → 调用 loader（比如查数据库）
     *   ③ SET key value PX {ttl}
     *
     * 典型使用场景：先查 Redis，缓存命中直接返回；未命中则查 DB 并回填缓存
     *
     * ⚠️ 注意：这里存在"缓存击穿"风险（大量并发同时 miss 时会同时打到 DB）
     *          生产环境可配合分布式锁（executeWithLock）解决
     *
     * @param key    缓存键
     * @param ttl    缓存过期时间
     * @param loader 数据加载函数，入参是 key，返回值是要缓存的对象
     */
    public <T> T getOrLoad(String key, Duration ttl, Function<String, T> loader) {
        RBucket<T> bucket = redissonClient.getBucket(key);

        // ① 先尝试从 Redis 读取
        // 等价命令：GET key
        T value = bucket.get();

        if (value == null) {
            // ② 缓存未命中，调用 loader（通常是查数据库）
            value = loader.apply(key);

            if (value != null) {
                // ③ 将加载到的数据写入 Redis 缓存，设置过期时间
                // 等价命令：SET key value PX {ttl的毫秒数}
                bucket.set(value, ttl);
            }
        }
        return value;
    }

    /**
     * 删除键
     *
     * 等价 Redis 命令：DEL key
     * 示例：DEL user:name  → 1（成功删除返回 true，键不存在返回 false）
     */
    public boolean delete(String key) {
        // getBucket(key).delete()：删除指定键
        // 等价命令：DEL key
        return redissonClient.getBucket(key).delete();
    }

    /**
     * 检查键是否存在
     *
     * 等价 Redis 命令：EXISTS key
     * 示例：EXISTS user:name  → 1（存在）或 0（不存在）
     *        Redisson 将 1/0 映射成 boolean true/false
     */
    public boolean exists(String key) {
        // isExists()：检查键是否存在
        // 等价命令：EXISTS key
        return redissonClient.getBucket(key).isExists();
    }

    /**
     * 设置过期时间（对已存在的 key 单独设置 TTL）
     *
     * 等价 Redis 命令：PEXPIRE key milliseconds
     * 示例：PEXPIRE user:name 3600000   （为已有的 key 追加1小时过期时间）
     *
     * 使用场景：key 已经存在，但忘记设过期时间，或者需要刷新过期时间
     */
    public boolean expire(String key, Duration ttl) {
        // expire(ttl)：为已存在的 key 设置/更新过期时间
        // 等价命令：PEXPIRE key {ttl的毫秒数}
        //           （若用秒为单位则是 EXPIRE key {seconds}）
        return redissonClient.getBucket(key).expire(ttl);
    }

    /**
     * 获取剩余过期时间（毫秒）
     *
     * 等价 Redis 命令：PTTL key
     * 返回值说明：
     *   正数  → 剩余毫秒数
     *   -1    → key 存在但没有设置过期时间（永久）
     *   -2    → key 不存在
     */
    public long getTimeToLive(String key) {
        // remainTimeToLive()：查询剩余过期时间，单位毫秒
        // 等价命令：PTTL key
        //           （若只需要秒级精度，原始命令是 TTL key）
        return redissonClient.getBucket(key).remainTimeToLive();
    }

    // ==================== Hash 操作 ====================
    // Hash 类型：一个 key 对应一张"小字典"，每个字典有多个 field-value 对
    // 适合存储结构化对象，比如 user:1 → {name:"alice", age:"30", email:"..."}

    /**
     * 设置 Hash 字段
     *
     * 等价 Redis 命令：HSET key field value
     * 示例：HSET user:1 name "alice"
     *        HSET user:1 age 30
     */
    public <K, V> void hSet(String key, K field, V value) {
        // getMap(key)：获取该 key 对应的 Hash 结构对象（RMap）
        // RMap 的泛型 <K, V> 对应 Hash 的 <field类型, value类型>
        RMap<K, V> map = redissonClient.getMap(key);

        // map.put(field, value)：向 Hash 中写入一个字段
        // 等价命令：HSET key field value
        // 注意：如果 field 已存在则覆盖，返回旧值（Redisson 的 put 也遵循此规则）
        map.put(field, value);
    }

    /**
     * 获取 Hash 字段
     *
     * 等价 Redis 命令：HGET key field
     * 示例：HGET user:1 name  → "alice"（字段不存在返回 nil）
     */
    public <K, V> V hGet(String key, K field) {
        RMap<K, V> map = redissonClient.getMap(key);

        // map.get(field)：读取 Hash 中指定 field 的值
        // 等价命令：HGET key field
        return map.get(field);
    }

    /**
     * 获取整个 Hash（所有字段和值）
     *
     * 等价 Redis 命令：HGETALL key
     * 示例：HGETALL user:1  → name alice age 30 email xxx
     *        Redisson 将其包装成 Map<K, V> 返回
     *
     * ⚠️ 注意：字段很多时慎用，会一次性加载所有字段到内存
     */
    public <K, V> Map<K, V> hGetAll(String key) {
        RMap<K, V> map = redissonClient.getMap(key);

        // readAllMap()：读取 Hash 中所有 field-value 对
        // 等价命令：HGETALL key
        return map.readAllMap();
    }

    /**
     * 删除 Hash 字段
     *
     * 等价 Redis 命令：HDEL key field
     * 示例：HDEL user:1 age  → 1（成功）或 0（字段不存在）
     *
     * @return true 表示字段存在并被删除，false 表示字段本来就不存在
     */
    public <K, V> boolean hDelete(String key, K field) {
        RMap<K, V> map = redissonClient.getMap(key);

        // map.remove(field)：删除指定 field，返回被删除的旧值（不存在则返回 null）
        // 等价命令：HDEL key field
        // 这里用 != null 判断是否真的删除了内容
        return map.remove(field) != null;
    }

    /**
     * 检查 Hash 字段是否存在
     *
     * 等价 Redis 命令：HEXISTS key field
     * 示例：HEXISTS user:1 name  → 1（存在）或 0（不存在）
     */
    public <K> boolean hExists(String key, K field) {
        RMap<K, Object> map = redissonClient.getMap(key);

        // containsKey(field)：检查 Hash 中是否存在指定 field
        // 等价命令：HEXISTS key field
        return map.containsKey(field);
    }

    // ==================== 分布式锁 ====================
    // Redis 分布式锁原理：利用 Redis 单线程特性保证"只有一个客户端能成功 SET"
    //
    // 原始 Redis 命令：
    //   SET lock:key "unique_value" NX PX 30000
    //   ├── NX：Not eXists，只在 key 不存在时才 SET（抢锁）
    //   ├── PX 30000：30秒后自动过期（防止客户端崩溃导致死锁）
    //   └── unique_value：唯一标识（用于释放时验证是不是自己的锁）
    //
    // 释放锁（原子 Lua 脚本，防止删掉别人的锁）：
    //   if redis.call("get", KEYS[1]) == ARGV[1] then
    //       return redis.call("del", KEYS[1])
    //   else return 0 end
    //
    // Redisson 在此基础上增加了：
    //   ① 看门狗（Watchdog）：定时续期，防止业务未完成锁就过期
    //   ② 可重入：同一线程可以多次获取同一把锁（类似 ReentrantLock）
    //   ③ 公平锁、读写锁等高级特性

    /**
     * 获取锁对象（不立即加锁，需调用 lock() 或 tryLock()）
     *
     * 等价 Redis 命令（加锁时执行）：
     *   SET lock:{lockKey} {线程ID+UUID} NX PX 30000
     *
     * 使用方式：
     *   RLock lock = getLock("order:lock");
     *   lock.lock();          // 阻塞直到获锁
     *   try { ... }
     *   finally { lock.unlock(); }
     */
    public RLock getLock(String lockKey) {
        // getLock(lockKey)：获取一个可重入锁对象（未加锁）
        // 等价命令（加锁动作）：SET lock:lockKey value NX PX 30000
        // 注意：getLock 只是创建本地对象，并不会操作 Redis，真正操作发生在 lock() 调用时
        return redissonClient.getLock(lockKey);
    }

    /**
     * 尝试获取锁（非阻塞，立即返回结果）
     *
     * 等价 Redis 命令：SET lock:{lockKey} value NX PX {leaseTime}
     *
     * @param lockKey   锁的键名
     * @param waitTime  等待获锁的最大时间（超时则放弃，返回 false）
     * @param leaseTime 锁的持有时间（超时自动释放，防死锁）
     *                  传 -1 则启用看门狗自动续期
     * @param unit      时间单位
     * @return true 获锁成功，false 获锁失败（锁已被别人持有）
     */
    public boolean tryLock(String lockKey, long waitTime, long leaseTime, TimeUnit unit) {
        RLock lock = redissonClient.getLock(lockKey);
        try {
            // lock.tryLock(waitTime, leaseTime, unit)：
            //   在 waitTime 内自旋等待，若期间能获锁则返回 true
            //   等价命令：SET lock:lockKey value NX PX {leaseTime转毫秒}
            //             （waitTime 控制本地等待时长，不直接对应 Redis 命令参数）
            return lock.tryLock(waitTime, leaseTime, unit);
        } catch (InterruptedException e) {
            // 等待过程中线程被中断（如应用关闭），恢复中断状态并返回失败
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 释放锁
     *
     * 等价 Redis 命令（Lua 脚本原子执行）：
     *   if GET lock:{lockKey} == {当前线程持有的value}
     *     then DEL lock:{lockKey}
     *   end
     *
     * isHeldByCurrentThread()：防止释放不属于自己的锁（比如锁已过期被别人拿走了）
     */
    public void unlock(String lockKey) {
        RLock lock = redissonClient.getLock(lockKey);

        // isHeldByCurrentThread()：判断当前线程是否持有这把锁
        // 等价命令：GET lock:{lockKey}，然后比较 value 是否包含当前线程标识
        if (lock.isHeldByCurrentThread()) {
            // lock.unlock()：释放锁
            // 等价命令（Lua 原子脚本）：先 GET 验证是自己的锁，再 DEL 删除
            lock.unlock();
        }
    }

    /**
     * 执行带锁的操作（模板方法，自动处理加锁/解锁，推荐使用）
     *
     * 内部流程：
     *   ① 尝试获锁：SET lock:{lockKey} value NX PX {leaseTime}
     *   ② 获锁成功 → 执行 operation.execute()
     *   ③ 无论是否异常，finally 块都会释放锁：DEL lock:{lockKey}（Lua原子）
     *   ④ 获锁失败 → 抛出 BusinessException
     *
     * @param lockKey   锁的键名
     * @param waitTime  等待时间
     * @param leaseTime 锁持有时间
     * @param unit      时间单位
     * @param operation 需要在锁保护下执行的业务逻辑（Lambda 表达式）
     */
    public <T> T executeWithLock(String lockKey, long waitTime, long leaseTime,
                                 TimeUnit unit, LockedOperation<T> operation) {
        RLock lock = redissonClient.getLock(lockKey);
        try {
            // 尝试获取锁（带等待和超时）
            // 等价命令：SET lock:{lockKey} value NX PX {leaseTime转毫秒}
            if (lock.tryLock(waitTime, leaseTime, unit)) {
                try {
                    // 获锁成功，执行业务逻辑
                    return operation.execute();
                } finally {
                    // 业务结束后释放锁（finally 保证一定执行，即使业务抛异常）
                    // 等价命令（Lua 原子脚本）：GET验证 → DEL 删除
                    lock.unlock();
                }
            }
            // 在 waitTime 内未能获锁，抛出业务异常
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "获取锁失败: " + lockKey);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "获取锁被中断: " + lockKey, e);
        }
    }

    /**
     * 锁保护操作的函数式接口（无参数，有返回值）
     * 使用 Lambda 传入具体业务逻辑，例如：
     *   executeWithLock("lock:order", 3, 30, SECONDS, () -> createOrder(dto));
     */
    @FunctionalInterface
    public interface LockedOperation<T> {
        T execute();
    }

    // ==================== Stream 消息队列 ====================
    // Redis Stream 是 5.0 版本引入的持久化消息队列，类似简化版 Kafka
    //
    // 核心概念：
    //   Stream（流）   ：消息容器，可以有多个消费者组
    //   ConsumerGroup ：消费者组，同一组内消息只会被一个消费者处理（负载均衡）
    //   Consumer      ：消费者，属于某个组
    //   MessageId     ：消息ID，格式为"时间戳-序号"，例如 1749000000000-0
    //   PEL           ：Pending Entry List，已投递但未 ACK 的消息列表
    //
    // 生产流程：XADD → 消息进 Stream
    // 消费流程：XREADGROUP → 取出消息（进 PEL）→ 处理 → XACK → 从 PEL 移除

    /**
     * Stream 消息处理器接口
     * 使用方传入自定义实现，用于处理每条消息的业务逻辑
     */
    @FunctionalInterface
    public interface StreamMessageProcessor {
        void process(StreamMessageId messageId, Map<String, String> data);
    }

    /**
     * 消费 Stream 消息（阻塞模式）
     *
     * 等价 Redis 命令：
     *   XREADGROUP GROUP {groupName} {consumerName}
     *              COUNT {count}
     *              BLOCK {blockTimeoutMs}
     *              STREAMS {streamKey} >
     *
     * 参数说明：
     *   >  ：特殊符号，表示"只读取从未投递给任何消费者的新消息"
     *   BLOCK ：服务端阻塞等待，有新消息立即返回，比客户端轮询更高效
     *           blockTimeoutMs=0 表示永久阻塞直到有消息
     *
     * @param streamKey      Stream 键
     * @param groupName      消费者组名
     * @param consumerName   消费者名（通常用主机名+线程ID区分）
     * @param count          每次最多读取的消息数量
     * @param blockTimeoutMs 阻塞等待超时（毫秒），0 表示无限等待
     * @param processor      每条消息的处理逻辑
     * @return true 本次读取到消息并处理，false 超时无消息
     */
    public boolean streamConsumeMessages(
            String streamKey,
            String groupName,
            String consumerName,
            int count,
            long blockTimeoutMs,
            StreamMessageProcessor processor) {

        // getStream(streamKey, StringCodec.INSTANCE)：
        //   获取 Stream 结构对象，StringCodec 指定消息的 key/value 都用字符串编解码
        //   （不加 StringCodec 默认用 Java 序列化，对应 Redis 里就是二进制乱码）
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);

        Map<StreamMessageId, Map<String, String>> messages;
        try {
            // stream.readGroup(...)：以消费者组模式读取消息
            // 等价命令：
            //   XREADGROUP GROUP {groupName} {consumerName}
            //              COUNT {count}
            //              BLOCK {blockTimeoutMs}
            //              STREAMS {streamKey} >
            //
            // neverDelivered()  → 对应末尾的 >，只取新消息
            // count(count)      → 对应 COUNT 参数，限制单次读取数量
            // timeout(Duration) → 对应 BLOCK 参数，服务端等待时长
            messages = stream.readGroup(
                    groupName,
                    consumerName,
                    StreamReadGroupArgs.neverDelivered()
                            .count(count)
                            .timeout(Duration.ofMillis(blockTimeoutMs))
            );
        } catch (ClassCastException e) {
            // Redisson 4.0.0 的已知 Bug：
            //   Stream 中没有消息时，Redis 服务端返回空数组，
            //   但 Redisson 内部错误地把它强转成 Map 导致 ClassCastException
            //   这里捕获后静默处理，等价于"本次无新消息"
            return false;
        }

        if (messages == null || messages.isEmpty()) {
            // 超时没有消息（正常情况）
            return false;
        }

        // 遍历所有读取到的消息，逐条交给 processor 处理
        // 注意：processor 内部通常还需要调用 streamAck 确认消息
        //       否则消息会一直留在 PEL（待处理列表），重启后会被重新投递
        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            processor.process(entry.getKey(), entry.getValue());
        }

        return true;
    }

    /**
     * 创建消费者组（如果不存在则创建，已存在则忽略）
     *
     * 等价 Redis 命令：
     *   XGROUP CREATE {streamKey} {groupName} 0 MKSTREAM
     *   ├── 0       ：从 Stream 最早的消息开始消费（$ 表示只消费新消息）
     *   └── MKSTREAM：如果 Stream 不存在则自动创建（不加此参数 Stream 不存在会报错）
     *
     * 通常在应用启动时调用一次，确保消费者组存在
     */
    public void createStreamGroup(String streamKey, String groupName) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        try {
            // stream.createGroup(...)：创建消费者组
            // 等价命令：XGROUP CREATE {streamKey} {groupName} 0 MKSTREAM
            // name(groupName)   → 消费者组名
            // makeStream()      → 对应 MKSTREAM，Stream 不存在时自动创建
            stream.createGroup(StreamCreateGroupArgs.name(groupName).makeStream());
            log.info("创建 Stream 消费者组: stream={}, group={}", streamKey, groupName);
        } catch (Exception e) {
            // 如果消费者组已存在，Redis 返回 BUSYGROUP 错误
            // 这不是真正的错误，直接忽略即可
            if (e instanceof org.redisson.client.RedisException
                    && e.getMessage() != null
                    && e.getMessage().contains("BUSYGROUP")) {
                // BUSYGROUP：Redis 的错误码，表示"Consumer Group name already exists"
                // 直接 return，不需要处理
                return;
            }
            log.warn("创建消费者组失败: stream={}, group={}, error={}", streamKey, groupName, e.getMessage());
        }
    }

    /**
     * 发送消息到 Stream（不限制长度）
     *
     * 等价 Redis 命令：XADD {streamKey} * {field1} {value1} {field2} {value2} ...
     * 其中 * 表示让 Redis 自动生成消息 ID（格式：时间戳-序号，如 1749000000000-0）
     *
     * @param streamKey Stream 键
     * @param message   消息内容，是一个 Map（field → value），类似 HTTP 请求体
     * @return 消息 ID 字符串（格式：毫秒时间戳-序号）
     */
    public String streamAdd(String streamKey, Map<String, String> message) {
        return streamAdd(streamKey, message, 0);
    }

    /**
     * 发送消息到 Stream（带长度限制，适合生产环境防止内存无限增长）
     *
     * 等价 Redis 命令：
     *   XADD {streamKey} MAXLEN ~ {maxLen} * {field1} {value1} ...
     *   ├── MAXLEN ~ {maxLen}：限制 Stream 最大长度，超过时删除最旧的消息
     *   └── ~（波浪线）      ：非精确裁剪（Trim non-strict），性能更好，
     *                          实际长度可能略大于 maxLen（Redis 内部以 listpack 为单位删除）
     *
     * @param streamKey Stream 键
     * @param message   消息内容（Map 格式）
     * @param maxLen    最大长度，超过时自动裁剪旧消息（0 表示不限制）
     * @return 消息 ID 字符串
     */
    public String streamAdd(String streamKey, Map<String, String> message, int maxLen) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);

        // StreamAddArgs.entries(message)：构造 XADD 的参数
        // 等价命令（maxLen=0）：XADD {streamKey} * field1 value1 field2 value2
        StreamAddArgs<String, String> args = StreamAddArgs.entries(message);
        if (maxLen > 0) {
            // trimNonStrict()：对应 ~ 符号（非精确裁剪）
            // maxLen(maxLen)  ：对应 MAXLEN 后的数字
            // 等价命令：XADD {streamKey} MAXLEN ~ {maxLen} * field1 value1
            args.trimNonStrict().maxLen(maxLen);
        }

        // stream.add(args)：执行 XADD，返回自动生成的消息 ID
        // 等价命令：XADD {streamKey} [MAXLEN ~ maxLen] * field1 value1 ...
        StreamMessageId messageId = stream.add(args);
        log.debug("发送 Stream 消息: stream={}, messageId={}, maxLen={}", streamKey, messageId, maxLen);
        return messageId.toString();
    }

    /**
     * 从 Stream 读取消息（消费者组模式，非阻塞）
     *
     * 等价 Redis 命令：
     *   XREADGROUP GROUP {groupName} {consumerName} COUNT {count} STREAMS {streamKey} >
     *   （无 BLOCK 参数，立即返回，没消息则返回空）
     *
     * 与 streamConsumeMessages 的区别：
     *   此方法是非阻塞的（轮询模式），调用方需要自己写循环
     *   streamConsumeMessages 带 BLOCK，Redis 服务端挂起连接等待，更高效
     *
     * @return 消息 Map：key 是消息ID，value 是消息内容（field→value）
     */
    public Map<StreamMessageId, Map<String, String>> streamReadGroup(
            String streamKey, String groupName, String consumerName, int count) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);

        // stream.readGroup(...)：非阻塞读取消费者组消息
        // 等价命令：XREADGROUP GROUP {groupName} {consumerName} COUNT {count} STREAMS {streamKey} >
        return stream.readGroup(groupName, consumerName,
                StreamReadGroupArgs.neverDelivered().count(count));
    }

    /**
     * 确认消息已处理（ACK）
     *
     * 等价 Redis 命令：XACK {streamKey} {groupName} {messageId1} {messageId2} ...
     *
     * 作用：将消息从 PEL（Pending Entry List 待处理列表）中移除
     * 如果不调用 XACK：
     *   - 消息会一直留在 PEL
     *   - 调用 XPENDING 可以看到未确认的消息
     *   - 使用 XAUTOCLAIM 可以将超时未确认的消息重新分配给其他消费者
     *
     * ⚠️ 一般在 processor.process() 成功执行后才调用，确保"至少一次"投递语义
     *    如果 process 失败，不调用 XACK，消息会被重新投递（可能导致重复消费，需幂等）
     *
     * @param ids 可变参数，支持一次确认多条消息
     */
    public void streamAck(String streamKey, String groupName, StreamMessageId... ids) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);

        // stream.ack(groupName, ids)：确认消息
        // 等价命令：XACK {streamKey} {groupName} {id1} {id2} ...
        stream.ack(groupName, ids);
    }

    /**
     * 获取 Stream 长度（消息总数）
     *
     * 等价 Redis 命令：XLEN {streamKey}
     * 示例：XLEN mystream  → 1024（当前 Stream 中有1024条消息）
     *
     * 注意：XLEN 返回的是 Stream 总消息数，不是未消费数
     */
    public long streamLen(String streamKey) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);

        // stream.size()：获取 Stream 中的消息总数
        // 等价命令：XLEN {streamKey}
        return stream.size();
    }

    // ==================== 原子计数器 ====================
    // Redis 的 INCR/DECR 是原子操作（单线程执行），天然线程安全
    // 适合：访问计数、库存扣减、序列号生成、限流计数等

    /**
     * 获取原子计数器对象（适合需要多次操作同一计数器的场景）
     *
     * 等价 Redis 命令（计数器操作时）：
     *   INCR key / DECR key / GET key / SET key value
     *
     * 使用示例：
     *   RAtomicLong counter = getAtomicLong("page:views");
     *   counter.incrementAndGet();   // INCR
     *   counter.get();               // GET
     */
    public RAtomicLong getAtomicLong(String key) {
        // getAtomicLong(key)：获取原子长整型对象，对应 Redis String 类型（存储整数）
        return redissonClient.getAtomicLong(key);
    }

    /**
     * 自增并返回新值（原子操作）
     *
     * 等价 Redis 命令：INCR key
     * 示例：
     *   INCR page:views  → 1（第一次，key 不存在时从 0 开始自增）
     *   INCR page:views  → 2
     *   INCR page:views  → 3
     *
     * 特性：即使多个客户端并发 INCR，结果也绝对不会丢失（原子性）
     */
    public long increment(String key) {
        // incrementAndGet()：先自增，再返回新值（等价于 ++i）
        // 等价命令：INCR key
        return redissonClient.getAtomicLong(key).incrementAndGet();
    }

    /**
     * 自减并返回新值（原子操作）
     *
     * 等价 Redis 命令：DECR key
     * 示例：
     *   DECR stock:item:1  → 99（从100减到99）
     *   DECR stock:item:1  → 98
     *
     * 注意：Redis 不限制减到负数，如需防超卖请配合 Lua 脚本或分布式锁
     */
    public long decrement(String key) {
        // decrementAndGet()：先自减，再返回新值（等价于 --i）
        // 等价命令：DECR key
        return redissonClient.getAtomicLong(key).decrementAndGet();
    }

    // ==================== 列表操作 ====================
    // Redis List 是双端链表，支持头部/尾部的 O(1) 插入和删除
    // 适合：消息队列（简单场景）、最近记录、排行榜历史等

    /**
     * 从列表右侧添加元素（追加到尾部）
     *
     * 等价 Redis 命令：RPUSH key value
     * 示例：
     *   RPUSH recent:logins "alice"   → 列表: [alice]
     *   RPUSH recent:logins "bob"     → 列表: [alice, bob]
     *   RPUSH recent:logins "charlie" → 列表: [alice, bob, charlie]
     *
     * 注意：RPUSH 是 Right Push（右推），LPUSH 是 Left Push（左推）
     *       通常 RPUSH + LPOP 组合实现先进先出（FIFO）队列
     */
    public <T> void listRightPush(String key, T value) {
        // getList(key)：获取列表对象（RList），对应 Redis List 类型
        RList<T> list = redissonClient.getList(key);

        // list.add(value)：向列表末尾添加元素
        // 等价命令：RPUSH key value
        list.add(value);
    }

    /**
     * 获取列表所有元素
     *
     * 等价 Redis 命令：LRANGE key 0 -1
     *   0   ：起始下标（0是第一个元素）
     *   -1  ：结束下标（-1表示最后一个元素）
     *   0 -1：取全部，相当于 "SELECT * FROM list"
     *
     * 示例：LRANGE recent:logins 0 -1  → [alice, bob, charlie]
     *
     * ⚠️ 列表很长时慎用，会全部加载到内存
     */
    public <T> List<T> listGetAll(String key) {
        RList<T> list = redissonClient.getList(key);

        // readAll()：读取列表所有元素
        // 等价命令：LRANGE key 0 -1
        return list.readAll();
    }

    // ==================== 工具方法 ====================

    /**
     * 获取 RedissonClient 原始对象（用于调用本类未封装的高级功能）
     *
     * 例如：排序集合（ZSet/RankedSortedSet）、HyperLogLog、BitMap、
     *        地理位置（GEO）、布隆过滤器（RBloomFilter）等
     */
    public RedissonClient getClient() {
        return redissonClient;
    }

    /**
     * 按 Glob 模式批量删除键
     *
     * 等价 Redis 命令：
     *   内部使用 SCAN + DEL 组合（Redisson 封装，避免 KEYS 阻塞服务器）
     *   原始危险写法：KEYS user:* → DEL user:1 user:2 user:3...
     *
     * 示例：deleteByPattern("user:*")  → 删除所有 user: 开头的键
     *        deleteByPattern("cache:product:*")  → 删除所有商品缓存
     *
     * 支持的 Glob 通配符：
     *   *  匹配任意多个字符（如 user:*）
     *   ?  匹配单个字符（如 user:?）
     *   [] 匹配字符集（如 user:[123]）
     *
     * @return 被删除的键数量
     */
    public long deleteByPattern(String pattern) {
        // getKeys()：获取键管理对象
        RKeys keys = redissonClient.getKeys();

        // deleteByPattern(pattern)：按模式批量删除
        // 等价命令（安全版）：
        //   SCAN 0 MATCH {pattern} COUNT 100（循环直到 cursor 为 0）
        //   DEL {扫描到的所有键}
        return keys.deleteByPattern(pattern);
    }

    /**
     * 按 Glob 模式查找键（返回可迭代集合，懒加载，不会一次性全部加载到内存）
     *
     * 等价 Redis 命令：
     *   SCAN 0 MATCH {pattern} COUNT 100（分批扫描，游标迭代）
     *
     * ⚠️ 为什么不用 KEYS？
     *   KEYS {pattern} 会阻塞 Redis 单线程直到扫描完所有键（O(N) 全量扫描）
     *   键很多时会造成 Redis 服务几秒甚至几十秒无响应，生产事故高发区！
     *   SCAN 是增量迭代，每次只扫描一小部分，不会阻塞，是生产安全的替代方案
     *
     * @param pattern Glob 模式，例如 "user:*"、"session:??:*"
     * @return 可迭代的键集合（懒加载，遍历时才实际查询 Redis）
     */
    public Iterable<String> findKeysByPattern(String pattern) {
        RKeys keys = redissonClient.getKeys();

        // getKeys(KeysScanOptions.defaults().pattern(pattern))：按模式迭代键
        // 等价命令：SCAN 0 MATCH {pattern} COUNT 10（Redisson 内部自动处理游标循环）
        return keys.getKeys(KeysScanOptions.defaults().pattern(pattern));
    }
}