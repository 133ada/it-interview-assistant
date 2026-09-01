package interview.guide.modules.resume.listener;

import interview.guide.infrastructure.kafka.KafkaProperties;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.interview.model.ResumeAnalysisResponse;
import interview.guide.modules.resume.model.ResumeAnalyzeTask;
import interview.guide.modules.resume.model.ResumeEntity;
import interview.guide.modules.resume.repository.ResumeMessageDedupRepository;
import interview.guide.modules.resume.repository.ResumeRepository;
import interview.guide.modules.resume.service.ResumeGradingService;
import interview.guide.modules.resume.service.ResumePersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 简历分析 Kafka 消费者。
 * 负责从 Kafka Topic 消费任务并执行 AI 分析。
 * <p>
 * <b>幂等消费</b>：Kafka 通常只保证 at-least-once 投递，重复/重投消息通过唯一的 messageId
 * （DB 幂等表唯一约束）被拒，不会重复调用 LLM 或产生重复分析记录。
 * <p>
 * <b>认领锁</b>：处理前用 Redis 分布式锁抢占"消息级认领"，把并发处理同一条消息的线程/实例
 * 串行化，避免两个消费者都通过幂等快路径后各自调用一次 LLM。采用"有界等待 + 超时降级"：
 * 抢锁等待期间若抢占方完成，则获锁后 ACK 跳过；抢占方崩溃则锁随看门狗过期、由本线程接管；
 * 等待超时仍未拿到则降级为无锁处理（DB 唯一约束兜底），保证消息绝不因"跳过"而丢失。
 * <p>
 * <b>offset 提交时机</b>：手动提交（MANUAL_IMMEDIATE），仅当业务处理成功后才
 * {@code acknowledgment.acknowledge()}；失败抛出异常交给 {@code KafkaConfig} 中的
 * {@code DefaultErrorHandler} 指数退避重试（最多 maxAttempts 次），重试耗尽后写入死信队列
 * 并回调 {@link ResumeAnalyzeDlqHandler} 标记 FAILED。
 */
@Slf4j
@Component
public class AnalyzeKafkaConsumer {

    /**
     * 认领锁 key 前缀：{@code resume:analyze:claim:{messageId}}
     */
    private static final String CLAIM_LOCK_PREFIX = "resume:analyze:claim:";

    /**
     * 认领锁等待时间（秒）：有界等待抢占方完成或崩溃。
     * 与看门狗默认租约（30s）对齐，确保抢占方崩溃后本线程能在锁过期时接管；
     * 超时仍未拿到则降级为无锁处理（不跳过消息，见 {@link #onAnalyzeTask}）。
     */
    private static final long CLAIM_LOCK_WAIT_SECONDS = 30;

    /**
     * 认领锁租约：-1 = 启用 Redisson 看门狗自动续期（进程崩溃后锁自动过期）。
     */
    private static final long CLAIM_LOCK_LEASE_SECONDS = -1;

    /**
     * PostgreSQL 中语义为"该消息已完成/无需处理"，可直接跳过并提交 offset 的错误码：
     * 23505 = 唯一约束（并发重复），23503 = 外键（简历已删除）。
     */
    private static final Set<String> SKIP_SQL_STATES = Set.of("23505", "23503");

    private final ResumeGradingService gradingService;
    private final ResumePersistenceService resumePersistenceService;
    private final ResumeRepository resumeRepository;
    private final ResumeMessageDedupRepository dedupRepository;
    private final RedisService redisService;
    private final KafkaProperties kafkaProperties;

    public AnalyzeKafkaConsumer(
            ResumeGradingService gradingService,
            ResumePersistenceService resumePersistenceService,
            ResumeRepository resumeRepository,
            ResumeMessageDedupRepository dedupRepository,
            RedisService redisService,
            KafkaProperties kafkaProperties
    ) {
        this.gradingService = gradingService;
        this.resumePersistenceService = resumePersistenceService;
        this.resumeRepository = resumeRepository;
        this.dedupRepository = dedupRepository;
        this.redisService = redisService;
        this.kafkaProperties = kafkaProperties;
    }

    @KafkaListener(
            topics = "${app.kafka.resume-analyze-topic}",
            groupId = "${app.kafka.resume-analyze-group}"
    )
    public void onAnalyzeTask(@NonNull ResumeAnalyzeTask task,
                              @Header(KafkaHeaders.DELIVERY_ATTEMPT) int deliveryAttempt,
                              Acknowledgment acknowledgment) {
        String messageId = task.messageId();
        Long resumeId = task.resumeId();
        log.info("Processing 简历分析 task (deliveryAttempt={}/{}): resumeId={}, messageId={}",
                deliveryAttempt, kafkaProperties.getMaxAttempts(), resumeId, messageId);

        // 旧格式消息（本版本之前的存量，无 messageId）：无法幂等去重，ACK 跳过并告警。
        // 运营可通过 reanalyze 触发重新入队（此时会生成新的 messageId）
        if (messageId == null) {
            log.warn("旧格式消息无 messageId，跳过（请触发 reanalyze 重新入队）: resumeId={}", resumeId);
            acknowledgment.acknowledge();
            return;
        }

        // 1. 幂等快路径：该消息已处理过，直接 ACK 跳过，避免重复调用 LLM
        if (dedupRepository.existsByMessageId(messageId)) {
            log.info("消息已处理过，跳过: resumeId={}, messageId={}", resumeId, messageId);
            acknowledgment.acknowledge();
            return;
        }

        // 2. 抢占式认领：用分布式锁把 "同一消息的并发处理" 串行化，避免并发重复调用 LLM。
        //    刻意不采用"抢不到就跳过"的写法——那会在抢占方崩溃时，因后续消息 ACK 覆盖 offset 而丢消息。
        //    改为【有界等待 + 超时降级】：
        //      · 抢占方完成     → 获锁后重查幂等，直接 ACK 跳过；
        //      · 抢占方崩溃     → 看门狗停止续期，锁 ~30s 过期，本线程在等待窗口内接管重新处理；
        //      · 等待超时仍被持有 → 降级为无锁处理（DB 唯一约束兜底，最坏重复一次 LLM，不丢消息）。
        String claimLockKey = CLAIM_LOCK_PREFIX + messageId;
        boolean locked;
        try {
            locked = redisService.tryLock(claimLockKey, CLAIM_LOCK_WAIT_SECONDS, CLAIM_LOCK_LEASE_SECONDS, TimeUnit.SECONDS);
        } catch (Exception redisEx) {
            // Redis 不可用：降级为无锁处理。结果幂等仍由 DB 唯一约束（23505）兜底，
            // 最坏情况多调用一次 LLM，不会产生重复分析记录或丢消息。
            log.warn("获取认领锁失败，降级为无锁处理: messageId={}, error={}",
                    messageId, redisEx.getMessage(), redisEx);
            locked = true;
        }
        if (!locked) {
            // 争抢锁失败时：降级为无锁处理，保证消息不被跳过。
            // 此时双方都可能调用 LLM，但 recordAnalysisResult 的唯一约束保证只落一份结果。
            log.warn("认领锁等待超时，降级为无锁处理: resumeId={}, messageId={}", resumeId, messageId);
        }

        try {
            // 锁等待/降级期间该消息可能已被其他实例完成，获锁后再查一次幂等
            if (dedupRepository.existsByMessageId(messageId)) {
                log.info("消息已处理过（认领后确认），跳过: resumeId={}, messageId={}", resumeId, messageId);
                acknowledgment.acknowledge();
                return;
            }

            // 3. 异步处理前校验实体是否存在，不存在直接 ACK 丢弃
            if (!resumeRepository.existsById(resumeId)) {
                log.warn("简历已被删除，跳过分析任务: resumeId={}", resumeId);
                acknowledgment.acknowledge();
                return;
            }

            // 4. 标记 PROCESSING（独立短事务；失败即抛 → 重试 → DLQ）
            resumePersistenceService.markProcessing(resumeId);

            // 5. LLM 调用 —— 事务外执行（CLAUDE.md 禁止事务内调用外部 API）；失败抛 BusinessException
            ResumeAnalysisResponse analysis = gradingService.analyzeResume(task.content());

            // 6. 重载简历
            ResumeEntity resume = resumeRepository.findById(resumeId).orElse(null);
            if (resume == null) {
                log.warn("简历在分析期间被删除，跳过保存结果: resumeId={}", resumeId);
                acknowledgment.acknowledge();
                return;
            }

            // 7. 同一事务提交：幂等行 + 分析结果 + 状态 COMPLETED
            resumePersistenceService.recordAnalysisResult(messageId, resume, analysis);

            acknowledgment.acknowledge();
            log.info("简历分析任务完成: resumeId={}, messageId={}", resumeId, messageId);
        } catch (DataIntegrityViolationException e) {
            // 精确按 SQLState 区分，避免把非"重复/已删"的完整性异常误当已处理跳过：
            //   23505（唯一约束）= 并发重复，23503（简历 FK 因删除被触发）= 简历已删，二者都"跳过"；
            //   其余完整性异常（列超长、NOT NULL 等）是消息自身的缺陷，必须重抛走重试/DLQ。
            String sqlState = extractSqlState(e);
            if (SKIP_SQL_STATES.contains(sqlState)) {
                log.warn("检测到重复消息或简历已删除（sqlState={}），跳过并提交 offset: resumeId={}, messageId={}",
                        sqlState, resumeId, messageId, e);
                acknowledgment.acknowledge();
                return;
            }
            log.error("数据完整性异常（非重复/已删，不可跳过）: resumeId={}, messageId={}, sqlState={}",
                    resumeId, messageId, sqlState, e);
            throw e;
        } catch (Exception e) {
            // 重抛交给 DefaultErrorHandler：指数退避重试，最终进 DLQ 并标记 FAILED
            log.error("简历分析失败: resumeId={}, messageId={}, error={}",
                    resumeId, messageId, e.getMessage(), e);
            throw e;
        } finally {
            // 释放认领锁；unlock 失败（如 Redis 抖动）只告警不掩盖原结果/异常——锁有 TTL 兜底自动过期
            try {
                redisService.unlock(claimLockKey);
            } catch (Exception unlockEx) {
                log.warn("释放认领锁失败（锁将随 TTL 过期）: messageId={}, error={}",
                        messageId, unlockEx.getMessage(), unlockEx);
            }
        }
    }

    /**
     * 从异常链中提取底层 JDBC SQLState（PostgreSQL 错误码，如 23505/23503）。
     * Hibernate 约束异常被 Spring 翻译后根因仍是 {@link SQLException}，需沿 cause 链回溯。
     */
    private static String extractSqlState(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (t instanceof SQLException sqlException
                    && sqlException.getSQLState() != null
                    && !sqlException.getSQLState().isBlank()) {
                return sqlException.getSQLState();
            }
        }
        return null;
    }
}
