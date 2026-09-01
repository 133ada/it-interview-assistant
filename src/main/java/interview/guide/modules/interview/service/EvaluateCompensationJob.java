package interview.guide.modules.interview.service;

import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.modules.interview.listener.EvaluateStreamProducer;
import interview.guide.modules.interview.model.InterviewSessionEntity;
import interview.guide.modules.interview.repository.InterviewSessionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 面试评估补偿任务
 * <p>
 * 兜底 Redis Stream 投递链路上可能出现的"静默丢失"，保证最终一致：
 * 1. 生产者重试耗尽仍失败 → session 停留在 PENDING（见 {@link EvaluateStreamProducer # onSendFailed}）
 * 2. 消息已入队但消费者/Redis 故障，未及消费
 * <p>
 * 周期扫描 evaluateStatus=PENDING 且交卷（completedAt）超过阈值仍未推进的会话，重新投递。
 * 消费者对重复消息幂等（EVALUATED 跳过），补偿重投安全。
 */
@Slf4j
@Component
public class EvaluateCompensationJob {

    /**
     * 判定"滞留"的阈值：交卷后 5 分钟仍未开始/完成评估，认为消息可能在投递中丢失
     */
    private static final int STUCK_THRESHOLD_MINUTES = 5;

    /**
     * 评估租约时长：claim 抢占成功后 30 分钟仍未完成评估，认为执行者已崩溃，重置重投。
     * 阈值取较大值，避免误伤"慢但仍在执行"的 LLM 调用。
     */
    private static final int PROCESSING_LEASE_MINUTES = 30;

    private final InterviewSessionRepository sessionRepository;
    private final InterviewPersistenceService persistenceService;
    private final EvaluateStreamProducer evaluateStreamProducer;

    public EvaluateCompensationJob(
            InterviewSessionRepository sessionRepository,
            InterviewPersistenceService persistenceService,
            EvaluateStreamProducer evaluateStreamProducer
    ) {
        this.sessionRepository = sessionRepository;
        this.persistenceService = persistenceService;
        this.evaluateStreamProducer = evaluateStreamProducer;
    }

    /**
     * 每 60 秒扫描一次滞留的 PENDING 评估会话并重新投递。
     * fixedDelay：上一次执行结束再等 60 秒，避免任务重叠。
     */
    @Scheduled(fixedDelay = 60_000)
    public void resubmitStuckPendingEvaluations() {
        List<InterviewSessionEntity> stuckSessions = sessionRepository
                .findByEvaluateStatusAndCompletedAtBefore(
                        AsyncTaskStatus.PENDING,
                        LocalDateTime.now().minusMinutes(STUCK_THRESHOLD_MINUTES)
                );

        if (stuckSessions.isEmpty()) {
            return;
        }

        log.warn("补偿任务扫描到 {} 个滞留的 PENDING 评估会话，准备重新投递", stuckSessions.size());
        for (InterviewSessionEntity session : stuckSessions) {
            try {
                // 重新投递：生产者内部自带指数退避重试；仍失败则保持 PENDING 等下一轮补偿
                evaluateStreamProducer.sendEvaluateTask(session.getSessionId());
                log.info("补偿任务已重新投递评估任务: sessionId={}", session.getSessionId());
            } catch (Exception e) {
                // sendEvaluateTask 内部已捕获异常，此处兜底以防不可预期错误
                log.error("补偿任务重新投递失败: sessionId={}", session.getSessionId(), e);
            }
        }
    }

    /**
     * 每 60 秒扫描一次"租约过期"的 PROCESSING 评估会话（执行者崩溃卡死）并重置重投。
     * <p>
     * 解决 claim 无租约的唯一漏洞：消费者在 claim 成功后、markCompleted 前崩溃（OOM/被 kill），
     * 状态将永远停在 PROCESSING。此处将过期会话重置回 PENDING 并重新投递，
     * 恢复执行链路。若执行者只是慢（未超过 {@link #PROCESSING_LEASE_MINUTES}），
     * {@code resetStaleProcessing} 的 FOR UPDATE 复核会判定租约未过期，跳过不误伤。
     */
    @Scheduled(fixedDelay = 60_000)
    public void rescueStuckProcessingEvaluations() {
        List<InterviewSessionEntity> staleSessions = sessionRepository.findStaleProcessing(
                AsyncTaskStatus.PROCESSING,
                LocalDateTime.now().minusMinutes(PROCESSING_LEASE_MINUTES)
        );

        if (staleSessions.isEmpty()) {
            return;
        }

        log.warn("补偿任务扫描到 {} 个租约过期的 PROCESSING 评估会话，准备重置重投", staleSessions.size());
        for (InterviewSessionEntity session : staleSessions) {
            try {
                // 加锁复核并重置：仅在确认仍是过期 PROCESSING 时才回退，避免与"其实还在执行"的消费者竞争
                boolean reset = persistenceService.resetStaleProcessing(
                        session.getSessionId(),
                        LocalDateTime.now().minusMinutes(PROCESSING_LEASE_MINUTES)
                );
                if (reset) {
                    // 重置成功后重新投递，走 生产 → claim 抢占 → 执行 的正常链路
                    evaluateStreamProducer.sendEvaluateTask(session.getSessionId());
                    log.info("已重置并重新投递评估任务: sessionId={}", session.getSessionId());
                } else {
                    log.info("租约复核未通过，跳过重置: sessionId={}", session.getSessionId());
                }
            } catch (Exception e) {
                // 兜底：单个会话失败不影响本轮其余会话
                log.error("补偿任务重置失败: sessionId={}", session.getSessionId(), e);
            }
        }
    }


}
