package interview.guide.modules.resume.service;

import interview.guide.common.exception.BusinessException;
import interview.guide.infrastructure.kafka.KafkaProperties;
import interview.guide.modules.resume.listener.AnalyzeKafkaProducer;
import interview.guide.modules.resume.model.ResumeAnalyzeOutboxEntity;
import interview.guide.modules.resume.repository.ResumeRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 简历分析 Outbox 中继。
 * <p>
 * 周期扫描 PENDING 的 Outbox 任务并投递到 Kafka，解决"DB 更新成功但 Kafka 消息未发送"：
 * <ul>
 *   <li>投递成功 → 标记 SENT；</li>
 *   <li>投递重试耗尽且已写死信（生产者返回 false，简历状态已回写 FAILED）→ 标记 DLQ；</li>
 *   <li>投递失败且死信也失败（生产者抛 {@link BusinessException}）→ 保留 PENDING，重试计数 +1，下轮继续补偿（最终一致）
 *       若是重试次数；超过 {@link KafkaProperties#getOutboxMaxRetries()} 则标记 FAILED 停止空转。</li>
 * </ul>
 * <p>
 * 中继本身<b>非事务</b>：Kafka 发送不发生在数据库事务内（CLAUDE.md 禁止事务内调用外部 API）。
 * 每行独立 try/catch，单行失败不影响本轮其余行（风格参照 {@code EvaluateCompensationJob}）。
 */
@Slf4j
@Component
public class ResumeAnalyzeOutboxRelay {

    private final ResumeAnalyzeOutboxService outboxService;
    private final AnalyzeKafkaProducer analyzeKafkaProducer;
    private final ResumeRepository resumeRepository;
    private final KafkaProperties kafkaProperties;

    public ResumeAnalyzeOutboxRelay(
            ResumeAnalyzeOutboxService outboxService,
            AnalyzeKafkaProducer analyzeKafkaProducer,
            ResumeRepository resumeRepository,
            KafkaProperties kafkaProperties
    ) {
        this.outboxService = outboxService;
        this.analyzeKafkaProducer = analyzeKafkaProducer;
        this.resumeRepository = resumeRepository;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * 每轮扫描一次 PENDING 待投递任务。fixedDelay：上一次执行结束再等指定间隔，避免任务重叠。
     */
    @Scheduled(fixedDelayString = "${app.kafka.outbox-poll-interval-ms:5000}")
    public void pollPendingOutbox() {
        List<ResumeAnalyzeOutboxEntity> rows = outboxService.findPendingBatch(kafkaProperties.getOutboxBatchSize());
        if (rows.isEmpty()) {
            return;
        }
        log.info("Outbox 中继扫描到 {} 条待投递任务", rows.size());
        for (ResumeAnalyzeOutboxEntity row : rows) {
            try {
                processRow(row);
            } catch (BusinessException e) {
                // 投递失败且死信写入失败：保留 PENDING 下轮重试（最终一致）
                log.error("投递失败（保留 PENDING 下轮重试）: outboxId={}, resumeId={}, error={}",
                        row.getId(), row.getResumeId(), e.getMessage(), e);
                bumpRetryOrFail(row);
            } catch (Exception e) {
                // 不可预期错误：同样保留 PENDING，不中断本轮其余行
                log.error("处理 Outbox 行异常（下轮重试）: outboxId={}, resumeId={}",
                        row.getId(), row.getResumeId(), e);
                bumpRetryOrFail(row);
            }
        }
    }

    private void processRow(ResumeAnalyzeOutboxEntity row) {
        // 简历已删除：任务失去意义，直接清理 Outbox 行（消费者对这类消息本就会 ACK 丢弃）
        if (!resumeRepository.existsById(row.getResumeId())) {
            log.warn("简历已删除，清理 Outbox 行: outboxId={}, resumeId={}", row.getId(), row.getResumeId());
            outboxService.deleteRow(row.getId());
            return;
        }

        boolean sent = analyzeKafkaProducer.sendAnalyzeTask(
                row.getMessageId(), row.getResumeId(), row.getContent());
        if (sent) {
            outboxService.markSent(row.getId());
            log.info("Outbox 任务已投递: outboxId={}, resumeId={}, messageId={}",
                    row.getId(), row.getResumeId(), row.getMessageId());
        } else {
            // 已写入死信队列，简历状态已标记 FAILED：任务兜底完成
            outboxService.markDlq(row.getId(), "投递重试耗尽，已写入死信队列");
            log.warn("Outbox 任务进入死信队列: outboxId={}, resumeId={}, messageId={}",
                    row.getId(), row.getResumeId(), row.getMessageId());
        }
    }

    /**
     * 重试计数 +1；超过上限则标记 FAILED 停止空转（便于运营排查）。
     */
    private void bumpRetryOrFail(ResumeAnalyzeOutboxEntity row) {
        int retryCount = outboxService.incrementRetryCount(row.getId());
        if (retryCount >= kafkaProperties.getOutboxMaxRetries()) {
            outboxService.markFailed(row.getId(), "投递重试次数超过上限，停止自动重试");
            log.error("Outbox 任务重试超过上限，标记 FAILED: outboxId={}, resumeId={}",
                    row.getId(), row.getResumeId());
        }
    }
}
