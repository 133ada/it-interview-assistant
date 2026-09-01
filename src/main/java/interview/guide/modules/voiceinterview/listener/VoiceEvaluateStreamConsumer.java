package interview.guide.modules.voiceinterview.listener;

import interview.guide.common.async.AbstractStreamConsumer;
import interview.guide.common.config.AsyncConsumerProperties;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.voiceinterview.service.VoiceInterviewEvaluationService;
import interview.guide.modules.voiceinterview.service.VoiceInterviewService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 语音面试评估 Stream 消费者
 */
@Slf4j
@Component
public class VoiceEvaluateStreamConsumer extends AbstractStreamConsumer<VoiceEvaluateStreamConsumer.VoiceEvaluatePayload> {

    private final VoiceInterviewService voiceInterviewService;
    private final VoiceInterviewEvaluationService evaluationService;
    private final AsyncConsumerProperties consumerProperties;

    public VoiceEvaluateStreamConsumer(RedisService redisService,
                                       VoiceInterviewService voiceInterviewService,
                                       VoiceInterviewEvaluationService evaluationService,
                                       AsyncConsumerProperties consumerProperties) {
        super(redisService);
        this.voiceInterviewService = voiceInterviewService;
        this.evaluationService = evaluationService;
        this.consumerProperties = consumerProperties;
    }

    public record VoiceEvaluatePayload(String sessionId) {}

    @Override
    protected String taskDisplayName() {
        return "语音面试评估";
    }

    @Override
    protected String streamKey() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_STREAM_KEY;
    }

    @Override
    protected String groupName() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_GROUP_NAME;
    }

    @Override
    protected String consumerPrefix() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_CONSUMER_PREFIX;
    }

    @Override
    protected String threadName() {
        return "voice-evaluate-consumer";
    }

    @Override
    protected int consumerCount() {
        // 组内并行消费者数由配置 app.async.consumers.voice-evaluate 控制（默认 1）
        return consumerProperties.getVoiceEvaluate();
    }

    @Override
    protected VoiceEvaluatePayload parsePayload(StreamMessageId messageId, Map<String, String> data) {
        String sessionId = data.get(AsyncTaskStreamConstants.FIELD_VOICE_SESSION_ID);
        if (sessionId == null) {
            log.warn("消息格式错误，跳过: messageId={}", messageId);
            return null;
        }
        return new VoiceEvaluatePayload(sessionId);
    }

    @Override
    protected String payloadIdentifier(VoiceEvaluatePayload payload) {
        return "voiceSessionId=" + payload.sessionId();
    }

    @Override
    protected void markProcessing(VoiceEvaluatePayload payload) {
        voiceInterviewService.updateEvaluateStatus(
                Long.parseLong(payload.sessionId()), AsyncTaskStatus.PROCESSING, null);
    }

    @Override
    protected void processBusiness(VoiceEvaluatePayload payload) {
        evaluationService.generateEvaluation(Long.parseLong(payload.sessionId()));
        log.info("语音面试评估完成: sessionId={}", payload.sessionId());
    }

    @Override
    protected void markCompleted(VoiceEvaluatePayload payload) {
        voiceInterviewService.updateEvaluateStatus(
                Long.parseLong(payload.sessionId()), AsyncTaskStatus.COMPLETED, null);
    }

    @Override
    protected void markFailed(VoiceEvaluatePayload payload, String error) {
        voiceInterviewService.updateEvaluateStatus(
                Long.parseLong(payload.sessionId()), AsyncTaskStatus.FAILED, error);
    }

    @Override
    protected void retryMessage(VoiceEvaluatePayload payload, int retryCount, String error) {
        String sessionId = payload.sessionId();
        try {
            // 指数退避：首次重试 1s，随后 2s、4s...
            long backoffMs = (long) Math.pow(2, retryCount - 1) * 1000;
            String lastError = truncateError(error);

            Map<String, String> message = Map.of(
                AsyncTaskStreamConstants.FIELD_VOICE_SESSION_ID, sessionId,
                AsyncTaskStreamConstants.FIELD_RETRY_COUNT, String.valueOf(retryCount),
                AsyncTaskStreamConstants.FIELD_DUE_AT, String.valueOf(System.currentTimeMillis() + backoffMs),
                AsyncTaskStreamConstants.FIELD_LAST_ERROR, lastError != null ? lastError : "unknown"
            );

            redisService().streamAdd(
                AsyncTaskStreamConstants.VOICE_EVALUATE_STREAM_KEY,
                message,
                AsyncTaskStreamConstants.STREAM_MAX_LEN
            );
            log.warn("语音面试评估任务已重新入队（退避 {}ms）: sessionId={}, retryCount={}", backoffMs, sessionId, retryCount);
        } catch (Exception e) {
            log.error("重试入队失败: sessionId={}, error={}", sessionId, e.getMessage(), e);
            voiceInterviewService.updateEvaluateStatus(
                    Long.parseLong(sessionId), AsyncTaskStatus.FAILED, truncateError("重试入队失败: " + e.getMessage()));
        }
    }

    @Override
    protected String dlqKey() {
        return AsyncTaskStreamConstants.VOICE_EVALUATE_DLQ_KEY;
    }

    @Override
    protected Map<String, String> buildDlqMessage(VoiceEvaluatePayload payload, int retryCount, String error) {
        String lastError = truncateError(error);
        return Map.of(
            AsyncTaskStreamConstants.FIELD_VOICE_SESSION_ID, payload.sessionId(),
            AsyncTaskStreamConstants.FIELD_RETRY_COUNT, String.valueOf(retryCount),
            AsyncTaskStreamConstants.FIELD_LAST_ERROR, lastError != null ? lastError : "unknown",
            AsyncTaskStreamConstants.FIELD_FAILED_AT, String.valueOf(System.currentTimeMillis())
        );
    }
}
