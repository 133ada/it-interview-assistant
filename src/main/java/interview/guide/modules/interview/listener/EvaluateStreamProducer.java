package interview.guide.modules.interview.listener;

import interview.guide.common.async.AbstractStreamProducer;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.interview.service.InterviewPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 面试评估任务生产者
 * 作用：发送任务到 Redis Stream
 */
@Slf4j
@Component
public class EvaluateStreamProducer extends AbstractStreamProducer<String> {

    /**
     * MQ 投递最大重试次数
     */
    private static final int MAX_SEND_RETRIES = 3;

    private final InterviewPersistenceService persistenceService;

    public EvaluateStreamProducer(
            RedisService redisService,
            InterviewPersistenceService persistenceService
    ) {
        super(redisService); // 初始化 Redis 操作
        this.persistenceService = persistenceService;
    }

    /**
     * 对外方法：发送评估任务
     */
    public void sendEvaluateTask(String sessionId) {
        sendTask(sessionId); // 调用父类统一逻辑
    }

    @Override
    protected String taskDisplayName() {
        return "评估";
    }

    @Override
    protected String streamKey() {
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_STREAM_KEY; // Stream 名称
    }

    @Override
    protected Map<String, String> buildMessage(String sessionId) {
        // 构造消息体（key-value）
        return Map.of(
                AsyncTaskStreamConstants.FIELD_SESSION_ID, sessionId,
                AsyncTaskStreamConstants.FIELD_RETRY_COUNT, "0" // 初始重试次数
        );
    }

    @Override
    protected String payloadIdentifier(String sessionId) {
        return "sessionId=" + sessionId; // 日志标识
    }

    @Override
    protected void onSendFailed(String sessionId, String error) {
        // 投递失败 → 带指数退避的重试，不直接标记 FAILED。
        // 可以尝试的主要业务原因：评估任务对时效性不敏感（用户已交卷），短暂重试后可恢复。
        boolean sent = retrySendWithBackoff(sessionId);
        if (sent) {
            return;
        }
        // 所有重试均失败：保持 evaluateStatus = PENDING（由 submitAnswer 已写入），
        // 由 EvaluateCompensationJob 周期扫描滞留的 PENDING 会话并重新投递，保证最终一致。
        log.error("评估任务投递失败，由补偿任务兜底重投: sessionId={}", sessionId);

        // 记录失败原因（悲观锁 + 事务，与消费者状态写入同一收口），便于运维排查；
        persistenceService.updateEvaluateStatus(sessionId, AsyncTaskStatus.PENDING, "生产者端已经重新尝试投递三次，均失败: " + error);
    }

    /**
     * 指数退避重试投递，最多 {@link #MAX_SEND_RETRIES} 次。
     *
     * @return true 表示重试成功，false 表示全部失败
     */
    private boolean retrySendWithBackoff(String sessionId) {
        for (int i = 0; i < MAX_SEND_RETRIES; i++) {
            try {
                long backoffMs = (long) Math.pow(2, i) * 100; // 100ms, 200ms, 400ms, ...
                Thread.sleep(backoffMs);

                String messageId = redisService.streamAdd(
                        streamKey(),
                        buildMessage(sessionId),
                        AsyncTaskStreamConstants.STREAM_MAX_LEN
                );
                log.info("评估任务重试成功: sessionId={}, retry={}/{}, messageId={}",
                        sessionId, i + 1, MAX_SEND_RETRIES, messageId);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("评估任务重试被中断: sessionId={}", sessionId);
                return false;
            } catch (Exception e) {
                log.warn("评估任务重试失败: sessionId={}, retry={}/{}, error={}",
                        sessionId, i + 1, MAX_SEND_RETRIES, e.getMessage());
            }
        }
        return false;
    }


}




