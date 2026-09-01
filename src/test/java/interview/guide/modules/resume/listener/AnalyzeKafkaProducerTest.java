package interview.guide.modules.resume.listener;

import interview.guide.common.exception.BusinessException;
import interview.guide.infrastructure.kafka.KafkaProperties;
import interview.guide.modules.resume.model.ResumeAnalyzeTask;
import interview.guide.modules.resume.service.ResumePersistenceService;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link AnalyzeKafkaProducer} 单元测试：投递重试 + 死信兜底语义。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AnalyzeKafkaProducer 测试")
class AnalyzeKafkaProducerTest {

    private static final String TOPIC = "resume-analyze";
    private static final String DLQ_TOPIC = "resume-analyze-dlq";
    private static final long RESUME_ID = 100L;
    private static final String MESSAGE_ID = "msg-1";
    private static final String KEY = String.valueOf(RESUME_ID);

    @Mock
    private KafkaTemplate<String, ResumeAnalyzeTask> kafkaTemplate;
    @Mock
    private ResumePersistenceService resumePersistenceService;

    private KafkaProperties kafkaProperties;
    private AnalyzeKafkaProducer producer;

    @BeforeEach
    void setUp() {
        kafkaProperties = new KafkaProperties();
        kafkaProperties.setResumeAnalyzeTopic(TOPIC);
        kafkaProperties.setResumeAnalyzeDlqTopic(DLQ_TOPIC);
        // 2 次重试 + 首次投递 = 共 3 次发送；退避设小避免拖慢测试
        kafkaProperties.setSendMaxRetries(2);
        kafkaProperties.setSendRetryInitialBackoffMs(1);
        kafkaProperties.setSendRetryMultiplier(1.0);
        kafkaProperties.setSendDeliveryTimeoutMs(100);
        producer = new AnalyzeKafkaProducer(kafkaTemplate, kafkaProperties, resumePersistenceService);
    }

    private CompletableFuture<SendResult<String, ResumeAnalyzeTask>> okFuture() {
        SendResult<String, ResumeAnalyzeTask> result = mock(SendResult.class);
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(1L);
        when(result.getRecordMetadata()).thenReturn(metadata);
        return CompletableFuture.completedFuture(result);
    }

    private CompletableFuture<SendResult<String, ResumeAnalyzeTask>> failFuture(String message) {
        CompletableFuture<SendResult<String, ResumeAnalyzeTask>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException(message));
        return future;
    }

    @Nested
    @DisplayName("sendAnalyzeTask()")
    class SendAnalyzeTaskTests {

        @Test
        @DisplayName("发送成功返回 true，且不触达死信与状态回写")
        void successShouldReturnTrueAndNotTouchDlq() {
            when(kafkaTemplate.send(TOPIC, KEY, any(ResumeAnalyzeTask.class))).thenReturn(okFuture());

            boolean sent = producer.sendAnalyzeTask(MESSAGE_ID, RESUME_ID, "content");

            assertThat(sent).isTrue();
            verify(kafkaTemplate).send(TOPIC, KEY, any(ResumeAnalyzeTask.class));
            verify(kafkaTemplate, never()).send(eq(DLQ_TOPIC), anyString(), any(ResumeAnalyzeTask.class));
            verify(resumePersistenceService, never()).saveResumeEvaluateStateFAILED(any(), any());
        }

        @Test
        @DisplayName("重试耗尽且死信写入成功：返回 false，标记 FAILED 一次")
        void exhaustedWithDlqSuccessShouldReturnFalseAndMarkFailed() {
            when(kafkaTemplate.send(eq(TOPIC), eq(KEY), any(ResumeAnalyzeTask.class)))
                    .thenReturn(failFuture("broker down"));
            when(kafkaTemplate.send(eq(DLQ_TOPIC), eq(KEY), any(ResumeAnalyzeTask.class))).thenReturn(okFuture());

            boolean sent = producer.sendAnalyzeTask(MESSAGE_ID, RESUME_ID, "content");

            assertThat(sent).isFalse();
            // 业务 Topic 恰好投递 sendMaxRetries + 1 = 3 次
            verify(kafkaTemplate, times(3)).send(eq(TOPIC), eq(KEY), any(ResumeAnalyzeTask.class));
            verify(kafkaTemplate).send(DLQ_TOPIC, KEY, any(ResumeAnalyzeTask.class));
            verify(resumePersistenceService).saveResumeEvaluateStateFAILED(any(ResumeAnalyzeTask.class), any(Exception.class));
        }

        @Test
        @DisplayName("重试耗尽且死信写入也失败：抛 BusinessException，不标记 FAILED")
        void exhaustedWithDlqFailureShouldThrowAndNotMarkFailed() {
            when(kafkaTemplate.send(eq(TOPIC), eq(KEY), any(ResumeAnalyzeTask.class)))
                    .thenReturn(failFuture("broker down"));
            when(kafkaTemplate.send(eq(DLQ_TOPIC), eq(KEY), any(ResumeAnalyzeTask.class)))
                    .thenReturn(failFuture("dlq down"));

            assertThatThrownBy(() -> producer.sendAnalyzeTask(MESSAGE_ID, RESUME_ID, "content"))
                    .isInstanceOf(BusinessException.class);

            // 死信失败时不标记 FAILED：由中继保留 Outbox 为 PENDING 待补偿
            verify(resumePersistenceService, never()).saveResumeEvaluateStateFAILED(any(), any());
        }
    }
}
