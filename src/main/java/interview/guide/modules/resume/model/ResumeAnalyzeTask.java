package interview.guide.modules.resume.model;

/**
 * 简历分析任务消息体（Kafka JSON 序列化）。
 *
 * @param messageId 全局唯一消息 ID（在创建 Outbox 任务时生成并持久化，重投期间保持稳定，
 *                  消费者据此做幂等去重）
 * @param resumeId  简历 ID（同时作为 Kafka 消息 key，保证同一简历的消息进入同一分区）
 * @param content   简历文本快照
 */
public record ResumeAnalyzeTask(String messageId, Long resumeId, String content) {

}
