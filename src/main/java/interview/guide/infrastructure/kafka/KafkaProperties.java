package interview.guide.infrastructure.kafka;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Kafka 消息队列配置。
 * <p>
 * 当前仅简历分析模块使用 Kafka（其他模块仍走 Redis Stream），
 * 因此配置只覆盖简历分析相关的 Topic / 消费者组 / 重试策略。
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {

    /**
     * Kafka bootstrap servers 地址
     */
    private String bootstrapServers = "localhost:9092";

    /**
     * 简历分析 Topic
     */
    private String resumeAnalyzeTopic = "resume-analyze";

    /**
     * 简历分析死信 Topic（重试耗尽后写入，保留失败现场供排查/回放）
     */
    private String resumeAnalyzeDlqTopic = "resume-analyze-dlq";

    /**
     * 简历分析消费者组
     */
    private String resumeAnalyzeGroup = "resume-analyze-group";

    /**
     * 简历分析消费者并发数（建议不超过 Topic 分区数；默认 1 = 顺序消费）
     */
    private int consumerCount = 1;

    /**
     * 最大尝试次数（首次投递 + 最多 N-1 次重试）。
     * 例如 maxAttempts=3：共执行 3 次业务处理，之后进入最终失败（FAILED + DLQ）
     */
    private int maxAttempts = 3;

    /**
     * 首次重试退避间隔（毫秒）
     */
    private long retryInitialBackoffMs = 1000;

    /**
     * 重试退避倍数（指数退避）
     */
    private double retryMultiplier = 2.0;

    /**
     * 重试退避最大间隔（毫秒）
     */
    private long retryMaxBackoffMs = 16000;

    // ========== 生产者补偿配置（发送失败重试 + 死信） ==========

    /**
     * 生产者业务层重试次数：首次发送失败后，额外重试几次（与 Kafka 内部重试叠加）。
     * 例如 sendMaxRetries=2：共尝试 3 次发送，仍失败则进死信队列并标记 FAILED。
     */
    private int sendMaxRetries = 2;

    /**
     * 生产者业务层首次重试退避间隔（毫秒）
     */
    private long sendRetryInitialBackoffMs = 500;

    /**
     * 生产者业务层重试退避倍数（指数退避）
     */
    private double sendRetryMultiplier = 2.0;

    /**
     * 单次发送的最大交付时长（毫秒），对应 Kafka 生产者 delivery.timeout.ms：
     * Kafka 内部在该时长内自动重试，超时即判定本次发送失败。
     * 生产者等待发送结果时会在此基础上加缓冲，因此本值应小于等待缓冲之和。
     */
    private long sendDeliveryTimeoutMs = 5000;

    // ========== Outbox 中继配置 ==========

    /**
     * Outbox 中继轮询间隔（毫秒）
     */
    private long outboxPollIntervalMs = 5000;

    /**
     * Outbox 中继每批拉取的待投递任务数
     */
    private int outboxBatchSize = 50;

    /**
     * Outbox 投递重试次数上限：超过后标记 FAILED 停止自动重试（便于运营排查）
     */
    private int outboxMaxRetries = 100;
}
