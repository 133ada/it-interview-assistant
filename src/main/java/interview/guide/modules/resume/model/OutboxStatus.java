package interview.guide.modules.resume.model;

/**
 * 简历分析 Outbox 任务投递状态。
 */
public enum OutboxStatus {

    /**
     * 待投递（由中继 {@code ResumeAnalyzeOutboxRelay} 周期扫描投递到 Kafka）
     */
    PENDING,

    /**
     * 已投递到 Kafka 业务 Topic
     */
    SENT,

    /**
     * 投递重试耗尽，消息已写入死信队列（保留现场供回放/排查）
     */
    DLQ,

    /**
     * 投递重试次数超过上限，停止空转（需运营介入排查）
     */
    FAILED
}
