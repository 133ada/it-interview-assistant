package interview.guide.modules.resume.listener;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.infrastructure.kafka.KafkaProperties;
import interview.guide.modules.resume.model.ResumeAnalyzeTask;
import interview.guide.modules.resume.service.ResumePersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 简历分析任务生产者（Kafka）。
 * 由 Outbox 中继 {@code ResumeAnalyzeOutboxRelay} 调用，将待投递任务发送到 Kafka。
 * <p>
 * 投递补偿策略：
 * <ol>
 *   <li>Kafka 生产者内部自动重试（retries=3，delivery.timeout.ms 内）；</li>
 *   <li>仍失败则业务层指数退避重试（sendMaxRetries 次）；</li>
 *   <li>最终失败：同步写入死信 Topic {@code resume-analyze-dlq}（保留现场可回放），
 *       成功后回写 DB 状态 FAILED（兜底持久化）并返回 false（业务收口）；</li>
 *   <li>若死信写入也失败，抛 {@link BusinessException}：由中继保留 Outbox 为 PENDING，
 *       下轮继续补偿（最终一致）。</li>
 * </ol>
 */
@Slf4j
@Component
public class AnalyzeKafkaProducer {

    /**
     * 等待 Kafka 返回发送结果的缓冲时长（毫秒）。
     * 需大于 {@link KafkaProperties#getSendDeliveryTimeoutMs()}，确保单次尝试能拿到确定结果（成功或失败）。
     */
    private static final long SEND_WAIT_BUFFER_MS = 1000;

    /**
     * 死信发送的最大等待时间（秒）
     */
    private static final long DLQ_SEND_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, ResumeAnalyzeTask> kafkaTemplate;
    private final KafkaProperties kafkaProperties;
    private final ResumePersistenceService resumePersistenceService;

    public AnalyzeKafkaProducer(
            KafkaTemplate<String, ResumeAnalyzeTask> kafkaTemplate,
            KafkaProperties kafkaProperties,
            ResumePersistenceService resumePersistenceService
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
        this.resumePersistenceService = resumePersistenceService;
    }

    /**
     * 投递分析任务到 Kafka（失败自动重试，最终失败进死信队列并回写状态）。
     *
     * @return true=投递成功；false=投递失败但已写入死信队列（简历状态已标记 FAILED，任务已兜底）
     * @throws BusinessException 投递失败且死信写入也失败（不标记 FAILED，由中继保留任务待补偿）
     */
    public boolean sendAnalyzeTask(String messageId, Long resumeId, String content) {
        ResumeAnalyzeTask task = new ResumeAnalyzeTask(messageId, resumeId, content);

        Exception lastError = null;
        int totalAttempts = kafkaProperties.getSendMaxRetries() + 1;
        for (int attempt = 0; attempt < totalAttempts; attempt++) {
            lastError = trySend(task);
            if (lastError == null) {
                return true; // 发送成功
            }
            log.warn("投递简历分析任务失败（第 {}/{} 次）: resumeId={}, messageId={}, error={}",
                    attempt + 1, totalAttempts, resumeId, messageId, lastError.getMessage());
            if (attempt < totalAttempts - 1) {
                sleepQuietly(backoffMs(attempt));
            }
        }

        // 所有尝试均失败：进死信队列 + 标记 FAILED（死信写入成功才收口，失败抛异常）
        return onSendFailed(task, lastError);
    }

    /**
     * 单次发送，等待确定结果。成功返回 null，失败返回异常。
     */
    private Exception trySend(ResumeAnalyzeTask task) {
        try {
            SendResult<String, ResumeAnalyzeTask> result = kafkaTemplate
                    .send(
                            kafkaProperties.getResumeAnalyzeTopic(),
                            String.valueOf(task.resumeId()),
                            task
                    )
                    .get(kafkaProperties.getSendDeliveryTimeoutMs() + SEND_WAIT_BUFFER_MS, TimeUnit.MILLISECONDS);
            log.info("简历分析任务已投递到 Kafka: resumeId={}, partition={}, offset={}",
                    task.resumeId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Fixme 异常作为函数返回值返回出去
            return e;
        } catch (Exception e) {
            // Fixme 异常作为函数返回值返回出去
            return e;
        }
    }

    /**
     * 指数退避间隔：initial * multiplier^attempt
     */
    private long backoffMs(int attempt) {
        return (long) (kafkaProperties.getSendRetryInitialBackoffMs()
                * Math.pow(kafkaProperties.getSendRetryMultiplier(), attempt));
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 投递最终失败：同步写入死信队列；成功后回写 DB 状态 FAILED 并返回 false（业务收口）。
     */
    private boolean onSendFailed(ResumeAnalyzeTask task, Exception error) {
        // 写入死信队列
        sendToDlqSync(task);
        try {
            resumePersistenceService.saveResumeEvaluateStateFAILED(task, error);
        } catch (Exception e) {
            // 消息已在 DLQ 兜底，仅 DB 状态标记失败：不影响业务收口
            log.warn("标记简历分析 FAILED 失败（消息已在死信队列）: resumeId={}, error={}",
                    task.resumeId(), e.getMessage(), e);
        }
        return false;
    }

    /**
     * 将任务同步写入死信 Topic。写入失败抛 {@link BusinessException}。
     */
    private void sendToDlqSync(ResumeAnalyzeTask task) {
        String dlqTopic = kafkaProperties.getResumeAnalyzeDlqTopic();
        try {
            SendResult<String, ResumeAnalyzeTask> result = kafkaTemplate
                    .send(dlqTopic, String.valueOf(task.resumeId()), task)
                    .get(DLQ_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.warn("简历分析任务已写入死信队列: resumeId={}, messageId={}, topic={}, offset={}",
                    task.resumeId(), task.messageId(), dlqTopic, result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCode.RESUME_DLQ_SEND_FAILED,
                    "简历评估任务写入死信队列被中断: resumeId=" + task.resumeId(), e);
        } catch (Exception e) {
            log.error("写入死信队列失败: resumeId={}, messageId={}, error={}",
                    task.resumeId(), task.messageId(), e.getMessage(), e);
            throw new BusinessException(ErrorCode.RESUME_DLQ_SEND_FAILED,
                    "简历评估任务写入死信队列失败: resumeId=" + task.resumeId(), e);
        }
    }
}
