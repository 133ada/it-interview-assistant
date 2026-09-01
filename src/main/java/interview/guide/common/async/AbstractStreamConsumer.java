package interview.guide.common.async;

import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.infrastructure.redis.RedisService;
import jakarta.annotation.PostConstruct;   // Spring Bean 初始化回调
import jakarta.annotation.PreDestroy;      // Spring Bean 销毁回调
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean; // 线程安全的 boolean，无需 synchronized
import java.util.concurrent.atomic.AtomicInteger;  // 线程序号，用于给同消费者多线程命名

@Slf4j
// 泛型抽象类，T 是具体业务消息的 Payload 类型（如 ResumeTask, InterviewTask 等）
public abstract class AbstractStreamConsumer<T> {

    private final RedisService redisService;

    // 原子标志位，控制消费循环是否继续运行
    // 多线程可见，且 set/get 是原子操作，避免可见性问题
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ExecutorService executorService; // 消费线程池：线程数 = 组内消费者数

    // 本次实际启动的组内消费者线程数（用于 shutdown 日志）
    private int consumerCount = 1;

    // 线程序号：同一个消费者的多个线程用序号命名区分（如 evaluate-consumer-1/2/3）
    private final AtomicInteger threadSeq = new AtomicInteger(0);

    protected AbstractStreamConsumer(RedisService redisService) {
        this.redisService = redisService;
    }

    // =========================================================
    // Bean 初始化时自动执行
    // =========================================================
    @PostConstruct
    public void init() {
        // 组内消费者线程数：子类可覆盖 consumerCount()，默认 1（单消费者顺序消费）
        int count = Math.max(1, consumerCount());
        this.consumerCount = count;

        // 创建线程池（corePoolSize=count, maxPoolSize=count）
        // 每个线程扮演组内的一个 Redis 消费者，并行拉取、由 Redis 负载均衡分摊消息
        this.executorService = new ThreadPoolExecutor(
                count,                      // 核心线程数
                count,                      // 最大线程数
                0L,                         // 空闲超时（此处无意义，因为 core=max）
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),// 任务队列（无界，但实际只会提交 count 个任务）
                r -> {
                    // 自定义线程工厂：命名线程（带序号，便于线程 dump 区分）+ 设为守护线程
                    // 守护线程不会阻止 JVM 关闭
                    Thread t = new Thread(r, threadName() + "-" + threadSeq.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                },
                // 拒绝策略：队列满时抛异常
                new ThreadPoolExecutor.AbortPolicy()
        );

        // 消费者组只需创建一次（组是所有消费者线程共用的），在提交线程前准备好，避免 NOGROUP
        prepareConsumerGroup();

        running.set(true);
        // 提交 count 个消费循环任务，每个线程一个唯一 consumerName
        // Redis 组内按 consumerName 区分消息归属（PEL 记录），名字必须唯一，否则归属混乱
        for (int i = 0; i < count; i++) {
            String consumerName = consumerPrefix() + UUID.randomUUID().toString().substring(0, 8);
            executorService.submit(() -> consumeLoop(consumerName));
        }
        log.info("{} consumer started: consumerCount={}", taskDisplayName(), count);
    }

    // =========================================================
    // Bean 销毁时自动执行（应用关闭、重启时触发）
    // =========================================================
    @PreDestroy
    public void shutdown() {
        running.set(false);        // 通知消费循环退出
        if (executorService != null) {
            executorService.shutdown(); // 等待当前任务完成后关闭线程池
        }
        log.info("{} consumer stopped: consumerCount={}", taskDisplayName(), consumerCount);
    }

    // =========================================================
    // 消费者启动入口
    // =========================================================
    private void prepareConsumerGroup() {
        try {
            // 【Redisson】创建 Stream 消费者组
            // 对应原生 Redis 命令：
            //   XGROUP CREATE <streamKey> <groupName> $ MKSTREAM
            //   $ 表示从最新消息开始消费（不消费历史消息）
            //   MKSTREAM 表示如果 Stream 不存在则自动创建
            redisService.createStreamGroup(streamKey(), groupName());
            log.info("Redis Stream group is ready: {}", groupName());
        } catch (Exception e) {
            // 如果组已存在，Redis 会报错 "BUSYGROUP Consumer Group name already exists"
            // 这里 warn 即可，不影响后续消费，因为组已经存在就够了
            log.warn("Failed to prepare Redis Stream group: groupName={}", groupName(), e);
        }
    }

    // =========================================================
    // 消费主循环（死循环，直到 running=false）
    // 每个线程传入自己的 consumerName，对同一个组 XREADGROUP，
    // Redis 会按 consumer 做负载均衡，每条消息只投给组内一个消费者
    // =========================================================
    private void consumeLoop(String consumerName) {
        while (running.get()) {
            try {
                // 【Redisson】从 Stream 拉取消息，内部实现：
                // 对应原生 Redis 命令：
                //   XREADGROUP GROUP <groupName> <consumerName>
                //     COUNT <BATCH_SIZE>
                //     BLOCK <POLL_INTERVAL_MS>
                //     STREAMS <streamKey> >
                //
                // GROUP：指定消费者组
                // COUNT：每次最多拉取多少条（如 10）
                // BLOCK：阻塞等待毫秒数（如 2000ms），无消息时挂起而非空转
                // STREAMS key >：> 表示只取未投递的新消息
                //
                // 拉到消息后，对每条消息回调 this::processMessage
                redisService.streamConsumeMessages(
                        streamKey(),
                        groupName(),
                        consumerName,
                        AsyncTaskStreamConstants.BATCH_SIZE,
                        AsyncTaskStreamConstants.POLL_INTERVAL_MS,
                        this::processMessage       // 消息处理回调
                );
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    log.info("Consumer thread interrupted: consumerName={}", consumerName);
                    break; // 线程被中断，优雅退出
                }
                log.error("Failed to consume message: consumerName={}", consumerName, e);
            }
        }
    }

    // =========================================================
    // 单条消息处理（由 streamConsumeMessages 回调触发）
    // =========================================================
    private void processMessage(StreamMessageId messageId, Map<String, String> data) {
        // 将 Redis Stream 的 Map<String,String> 反序列化为业务对象 T
        // messageId 格式：时间戳-序号，如 "1715000000000-0"
        T payload = parsePayload(messageId, data);

        if (payload == null) {
            // 消息格式损坏，无法解析，直接 ACK 丢弃，避免无限重试卡死
            // 对应原生 Redis 命令：XACK <streamKey> <groupName> <messageId>
            ackMessage(messageId);
            return;
        }

        // 从消息 data 里读取重试次数字段（生产者写入时附带）
        int retryCount = parseRetryCount(data);
        log.info("Processing {} task: payload={}, messageId={}, retryCount={}",
                taskDisplayName(), payloadIdentifier(payload), messageId, retryCount);

        // 指数退避：重试消息携带 dueAt（最早可执行时间），未到时间先等待，
        // 避免失败后立即重试、在数秒内连续烧多次昂贵的 AI 调用。
        deferUntilDue(data);

        try {
            // 并发 / 幂等保护：先尝试 "抢占" （如 DB CAS 把 PENDING → PROCESSING）。
            // 抢占失败说明其他消费者正在处理同一业务或该业务已完成，直接跳过：
            // 按成功 ACK（不写状态、不触发重试），避免多实例重复执行昂贵的业务。
            if (!tryAcquire(payload)) {
                log.info("{} task skipped by acquire check: {}",
                        taskDisplayName(), payloadIdentifier(payload));
                ackMessage(messageId);
                return;
            }
            markProcessing(payload);  // 业务状态机：标记为"处理中"
            processBusiness(payload); // 执行核心业务逻辑（调用 AI、写库等）
            markCompleted(payload);   // 业务状态机：标记为"完成"

            // 成功处理后 ACK，通知 Redis 该消息已消费完毕
            // 对应原生 Redis 命令：XACK <streamKey> <groupName> <messageId>
            // ACK 后消息从 PEL（Pending Entry List）中移除，不会再被重投
            ackMessage(messageId);
            log.info("{} task completed: {}", taskDisplayName(), payloadIdentifier(payload));

        } catch (Exception e) {
            log.error("{} task failed: {}", taskDisplayName(), payloadIdentifier(payload), e);

            if ((retryCount + 1) < AsyncTaskStreamConstants.MAX_ATTEMPTS) {
                // 还有重试机会：业务层手动 XADD 一条新消息，携带 retryCount+1、退避时间、上次错误
                // 注意：这里不是利用 Redis 的 XCLAIM，而是业务层手动 XADD 新消息
                // 对应原生 Redis 命令（在 retryMessage 内部）：
                //   XADD <streamKey> * field1 val1 ... retryCount <retryCount+1>
                retryMessage(payload, retryCount + 1, e.getMessage());
            } else {
                // 已达最大尝试次数：标记为最终失败（DB 持久化），并写入死信队列（DLQ）保留失败现场
                markFailed(payload, truncateError(
                        taskDisplayName() + " failed after " + (retryCount + 1) + " attempts: " + e.getMessage()
                ));
                sendToDlq(payload, retryCount, e.getMessage());
            }

            // 无论重试还是最终失败，都 ACK 当前消息：
            //   失败结果已通过 DB 状态（FAILED）或 DLQ 持久化；
            //   不 ACK 只会让消息滞留 PEL，而 Redis Streams 不会自动重投 PEL 消息
            ackMessage(messageId);
        }
    }

    // =========================================================
    // 工具方法
    // =========================================================

    // 解析 retryCount 字段，容错处理（防止字段缺失或格式错误）
    protected int parseRetryCount(Map<String, String> data) {
        try {
            return Integer.parseInt(data.getOrDefault(AsyncTaskStreamConstants.FIELD_RETRY_COUNT, "0"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // 解析 dueAt（最早可执行时间戳），容错处理（防止字段缺失或格式错误）
    protected long parseDueAt(Map<String, String> data) {
        try {
            return Long.parseLong(data.getOrDefault(AsyncTaskStreamConstants.FIELD_DUE_AT, "0"));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    // 指数退避：未到重试时间则让出线程等待（单线程消费者，直接睡眠即可，退避上限很小）
    private void deferUntilDue(Map<String, String> data) {
        long dueAt = parseDueAt(data);
        if (dueAt <= 0) {
            return;
        }
        long waitMs = dueAt - System.currentTimeMillis();
        if (waitMs <= 0) {
            return;
        }
        try {
            log.debug("{} task 未到退避重试时间，等待: waitMs={}", taskDisplayName(), waitMs);
            Thread.sleep(waitMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("{} task 退避等待被中断", taskDisplayName());
        }
    }

    // 截断错误信息，防止超长字符串写入数据库
    protected String truncateError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 500 ? error.substring(0, 500) : error;
    }

    private void ackMessage(StreamMessageId messageId) {
        try {
            // 【Redisson】确认消息
            // 对应原生 Redis 命令：XACK <streamKey> <groupName> <messageId>
            redisService.streamAck(streamKey(), groupName(), messageId);
        } catch (Exception e) {
            log.error("Failed to ack stream message: messageId={}", messageId, e);
        }
    }

    // 将重试耗尽的消息写入死信队列（DLQ），保留失败现场，供 XRANGE 人工排查或后续回放
    protected void sendToDlq(T payload, int retryCount, String error) {
        try {
            // 对应原生 Redis 命令：XADD <dlqKey> MAXLEN ~ 1000 <fields...>
            redisService.streamAdd(
                    dlqKey(),
                    buildDlqMessage(payload, retryCount, error),
                    AsyncTaskStreamConstants.STREAM_MAX_LEN
            );
            log.warn("{} task sent to DLQ: payload={}, retryCount={}",
                    taskDisplayName(), payloadIdentifier(payload), retryCount);
        } catch (Exception e) {
            // DLQ 写入失败不影响主流程：DB 的 FAILED 状态已保留失败事实
            log.error("写入 DLQ 失败: payload={}, error={}",
                    payloadIdentifier(payload), e.getMessage(), e);
        }
    }

    // 暴露给子类使用 redisService（如 retryMessage 中 XADD）
    protected RedisService redisService() {
        return redisService;
    }

    // =========================================================
    // 可覆盖配置：组内消费者线程数
    // =========================================================

    /**
     * 该消费者在消费者组内启动的线程数（即组内 Redis 消费者的数量）。
     * 默认 1：单消费者顺序消费。
     * 子类覆盖返回 N（如 3），则组内 N 个消费者并行分摊消息、提高吞吐。
     * <p>
     * ⚠️ 注意：N &gt; 1 后，Redis 只保证"每条消息投给一个消费者"（负载均衡），
     * 不再保证组内全局处理顺序；并发执行同一业务需靠 {@link #tryAcquire} 的
     * 原子抢占/幂等钩子兜底，避免重复执行。
     */
    protected int consumerCount() {
        return 1;
    }

    // =========================================================
    // 子类必须实现的抽象方法（模板方法模式）
    // =========================================================
    protected abstract String taskDisplayName();        // 任务名，用于日志

    protected abstract String streamKey();              // Redis Stream Key

    protected abstract String groupName();              // 消费者组名

    protected abstract String consumerPrefix();         // 消费者名前缀

    protected abstract String threadName();             // 消费线程名

    protected abstract T parsePayload(StreamMessageId messageId, Map<String, String> data); // // 反序列化消息体

    protected abstract String payloadIdentifier(T payload); // 日志标识（如任务ID）

    /**
     * 幂等/并发"抢占"钩子（默认放行）。
     * 子类可覆盖为原子抢占（如 SELECT ... FOR UPDATE 把 PENDING 推进为 PROCESSING），
     * 用于防止多实例并发消费同一业务消息导致重复执行。
     *
     * @return true=允许继续处理；false=跳过本条消息（按成功 ACK，不写状态、不重试）
     */
    protected boolean tryAcquire(T payload) {
        return true; // 默认不拦截；需要并发抢占的消费者自行覆盖
    }

    protected abstract void markProcessing(T payload); // 状态：处理中

    protected abstract void processBusiness(T payload);// 核心业务逻辑

    protected abstract void markCompleted(T payload);  // 状态：完成

    protected abstract void markFailed(T payload, String error); // 状态：失败

    protected abstract void retryMessage(T payload, int retryCount, String error); // 重新投递（携带退避时间与上次错误）

    protected abstract String dlqKey(); // 死信队列 Stream Key

    protected abstract Map<String, String> buildDlqMessage(T payload, int retryCount, String error); // 死信消息体
}