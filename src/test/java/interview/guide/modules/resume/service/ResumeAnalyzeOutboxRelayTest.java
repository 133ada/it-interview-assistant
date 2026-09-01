package interview.guide.modules.resume.service;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.infrastructure.kafka.KafkaProperties;
import interview.guide.modules.resume.listener.AnalyzeKafkaProducer;
import interview.guide.modules.resume.model.ResumeAnalyzeOutboxEntity;
import interview.guide.modules.resume.repository.ResumeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * {@link ResumeAnalyzeOutboxRelay} 单元测试：投递成功/死信/保留重试/孤儿清理语义。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ResumeAnalyzeOutboxRelay 测试")
class ResumeAnalyzeOutboxRelayTest {

    @Mock
    private ResumeAnalyzeOutboxService outboxService;
    @Mock
    private AnalyzeKafkaProducer analyzeKafkaProducer;
    @Mock
    private ResumeRepository resumeRepository;

    private KafkaProperties kafkaProperties;
    private ResumeAnalyzeOutboxRelay relay;

    @BeforeEach
    void setUp() {
        kafkaProperties = new KafkaProperties();
        kafkaProperties.setOutboxBatchSize(10);
        kafkaProperties.setOutboxMaxRetries(5);
        relay = new ResumeAnalyzeOutboxRelay(outboxService, analyzeKafkaProducer, resumeRepository, kafkaProperties);
    }

    private ResumeAnalyzeOutboxEntity row(long id) {
        return ResumeAnalyzeOutboxEntity.builder()
                .id(id)
                .messageId("m-" + id)
                .resumeId(100L + id)
                .content("content-" + id)
                .build();
    }

    @Nested
    @DisplayName("pollPendingOutbox()")
    class PollPendingOutboxTests {

        @Test
        @DisplayName("空批次时无任何交互")
        void emptyBatchShouldDoNothing() {
            when(outboxService.findPendingBatch(10)).thenReturn(List.of());

            relay.pollPendingOutbox();

            verifyNoInteractions(analyzeKafkaProducer);
        }

        @Test
        @DisplayName("投递成功：标记 SENT")
        void sentShouldMarkSent() {
            when(outboxService.findPendingBatch(10)).thenReturn(List.of(row(1)));
            when(resumeRepository.existsById(101L)).thenReturn(true);
            when(analyzeKafkaProducer.sendAnalyzeTask("m-1", 101L, "content-1")).thenReturn(true);

            relay.pollPendingOutbox();

            verify(outboxService).markSent(1L);
            verify(outboxService, never()).markDlq(eq(1L), anyString());
        }

        @Test
        @DisplayName("投递重试耗尽已写死信：标记 DLQ")
        void dlqShouldMarkDlq() {
            when(outboxService.findPendingBatch(10)).thenReturn(List.of(row(1)));
            when(resumeRepository.existsById(101L)).thenReturn(true);
            when(analyzeKafkaProducer.sendAnalyzeTask("m-1", 101L, "content-1")).thenReturn(false);

            relay.pollPendingOutbox();

            verify(outboxService).markDlq(1L, "投递重试耗尽，已写入死信队列");
            verify(outboxService, never()).markSent(1L);
        }

        @Test
        @DisplayName("投递失败且死信失败（抛异常）：保留 PENDING，重试计数 +1，不标记 SENT/DLQ")
        void businessExceptionShouldKeepPendingAndBumpRetry() {
            when(outboxService.findPendingBatch(10)).thenReturn(List.of(row(1)));
            when(resumeRepository.existsById(101L)).thenReturn(true);
            when(analyzeKafkaProducer.sendAnalyzeTask("m-1", 101L, "content-1"))
                    .thenThrow(new BusinessException(ErrorCode.RESUME_DLQ_SEND_FAILED));
            when(outboxService.incrementRetryCount(1L)).thenReturn(1);

            relay.pollPendingOutbox();

            verify(outboxService).incrementRetryCount(1L);
            verify(outboxService, never()).markSent(1L);
            verify(outboxService, never()).markDlq(eq(1L), anyString());
            verify(outboxService, never()).markFailed(eq(1L), anyString());
        }

        @Test
        @DisplayName("重试计数超过上限：标记 FAILED 停止空转")
        void retryExceedingMaxShouldMarkFailed() {
            when(outboxService.findPendingBatch(10)).thenReturn(List.of(row(1)));
            when(resumeRepository.existsById(101L)).thenReturn(true);
            when(analyzeKafkaProducer.sendAnalyzeTask("m-1", 101L, "content-1"))
                    .thenThrow(new BusinessException(ErrorCode.RESUME_DLQ_SEND_FAILED));
            when(outboxService.incrementRetryCount(1L)).thenReturn(5);

            relay.pollPendingOutbox();

            verify(outboxService).markFailed(1L, "投递重试次数超过上限，停止自动重试");
        }

        @Test
        @DisplayName("简历已删除：清理 Outbox 行，不触达生产者")
        void deletedResumeShouldDeleteRow() {
            when(outboxService.findPendingBatch(10)).thenReturn(List.of(row(1)));
            when(resumeRepository.existsById(101L)).thenReturn(false);

            relay.pollPendingOutbox();

            verify(outboxService).deleteRow(1L);
            verifyNoInteractions(analyzeKafkaProducer);
        }

        @Test
        @DisplayName("单行异常不影响批内其余行")
        void oneThrowingRowShouldNotBreakBatch() {
            when(outboxService.findPendingBatch(10)).thenReturn(List.of(row(1), row(2)));
            when(resumeRepository.existsById(101L)).thenReturn(true);
            when(resumeRepository.existsById(102L)).thenReturn(true);
            when(analyzeKafkaProducer.sendAnalyzeTask("m-1", 101L, "content-1"))
                    .thenThrow(new BusinessException(ErrorCode.RESUME_DLQ_SEND_FAILED));
            when(analyzeKafkaProducer.sendAnalyzeTask("m-2", 102L, "content-2")).thenReturn(true);
            when(outboxService.incrementRetryCount(1L)).thenReturn(1);

            relay.pollPendingOutbox();

            verify(outboxService).incrementRetryCount(1L);
            verify(outboxService).markSent(2L);
        }
    }
}
