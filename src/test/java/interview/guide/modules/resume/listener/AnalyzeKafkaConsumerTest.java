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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.support.Acknowledgment;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link AnalyzeKafkaConsumer} 单元测试：幂等消费 + offset 提交时机 + 重试/DLQ 语义。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AnalyzeKafkaConsumer 测试")
class AnalyzeKafkaConsumerTest {

    private static final String MESSAGE_ID = "msg-1";
    private static final long RESUME_ID = 100L;
    private static final String CONTENT = "简历内容";

    @Mock
    private ResumeGradingService gradingService;
    @Mock
    private ResumePersistenceService persistenceService;
    @Mock
    private ResumeRepository resumeRepository;
    @Mock
    private ResumeMessageDedupRepository dedupRepository;
    @Mock
    private RedisService redisService;

    private KafkaProperties kafkaProperties;
    private AnalyzeKafkaConsumer consumer;

    @BeforeEach
    void setUp() {
        kafkaProperties = new KafkaProperties();
        kafkaProperties.setMaxAttempts(3);
        consumer = new AnalyzeKafkaConsumer(
                gradingService, persistenceService, resumeRepository, dedupRepository, redisService, kafkaProperties);
    }

    /** 认领锁成功获取（默认 stub，供各处理路径复用）。 */
    private void lockAcquired() {
        when(redisService.tryLock(anyString(), anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
    }

    private ResumeAnalysisResponse analysis() {
        return new ResumeAnalysisResponse(
                85,
                new ResumeAnalysisResponse.ScoreDetail(20, 17, 20, 13, 15),
                "summary",
                List.of("优点"),
                List.of(new ResumeAnalysisResponse.Suggestion("内容", "高", "问题", "建议")),
                CONTENT
        );
    }

    @Nested
    @DisplayName("onAnalyzeTask() 幂等与跳过")
    class IdempotencyTests {

        @Test
        @DisplayName("旧格式消息（无 messageId）：ACK 跳过，不调用 LLM")
        void nullMessageIdShouldAckAndSkip() {
            Acknowledgment ack = mock(Acknowledgment.class);

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(null, RESUME_ID, CONTENT), 1, ack);

            verify(ack).acknowledge();
            verify(gradingService, never()).analyzeResume(any());
        }

        @Test
        @DisplayName("消息已处理过：幂等快路径命中，ACK 跳过，不调用 LLM")
        void duplicateMessageShouldAckAndSkip() {
            Acknowledgment ack = mock(Acknowledgment.class);
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(true);

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack);

            verify(ack).acknowledge();
            verify(gradingService, never()).analyzeResume(any());
        }

        @Test
        @DisplayName("简历已删除：ACK 丢弃")
        void deletedResumeShouldAckAndSkip() {
            Acknowledgment ack = mock(Acknowledgment.class);
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(false);
            lockAcquired();

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack);

            verify(ack).acknowledge();
            verify(gradingService, never()).analyzeResume(any());
        }

        @Test
        @DisplayName("认领锁被其他实例持有且等待超时：降级为无锁处理，不丢消息，仍完成落库")
        void lockTimeoutShouldDegradeToUnlockedProcessing() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ResumeEntity resume = new ResumeEntity();
            resume.setId(RESUME_ID);
            ResumeAnalysisResponse analysis = analysis();
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(true);
            when(redisService.tryLock(anyString(), anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(false);
            when(gradingService.analyzeResume(CONTENT)).thenReturn(analysis);
            when(resumeRepository.findById(RESUME_ID)).thenReturn(Optional.of(resume));

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack);

            verify(persistenceService).recordAnalysisResult(MESSAGE_ID, resume, analysis);
            verify(ack).acknowledge();
        }
    }

    @Nested
    @DisplayName("onAnalyzeTask() 处理成功/失败")
    class ProcessingTests {

        @Test
        @DisplayName("处理成功：recordAnalysisResult 后 ACK")
        void happyPathShouldCommitResultThenAck() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ResumeEntity resume = new ResumeEntity();
            resume.setId(RESUME_ID);
            ResumeAnalysisResponse analysis = analysis();
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(true);
            when(gradingService.analyzeResume(CONTENT)).thenReturn(analysis);
            when(resumeRepository.findById(RESUME_ID)).thenReturn(Optional.of(resume));
            lockAcquired();

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack);

            verify(persistenceService).markProcessing(RESUME_ID);
            verify(persistenceService).recordAnalysisResult(MESSAGE_ID, resume, analysis);
            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("并发重复（唯一约束 23505）：ACK 跳过，不重抛（避免误入 DLQ）")
        void dataIntegrityViolationShouldAckAndSkip() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ResumeEntity resume = new ResumeEntity();
            resume.setId(RESUME_ID);
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(true);
            when(gradingService.analyzeResume(CONTENT)).thenReturn(analysis());
            when(resumeRepository.findById(RESUME_ID)).thenReturn(Optional.of(resume));
            lockAcquired();
            doThrow(new DataIntegrityViolationException("duplicate key",
                    new SQLException("duplicate key value violates unique constraint", "23505")))
                    .when(persistenceService).recordAnalysisResult(eq(MESSAGE_ID), any(), any());

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack);

            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("简历 FK 约束冲突（23503）：视为简历已删，ACK 跳过")
        void foreignKeyViolationShouldAckAndSkip() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ResumeEntity resume = new ResumeEntity();
            resume.setId(RESUME_ID);
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(true);
            when(gradingService.analyzeResume(CONTENT)).thenReturn(analysis());
            when(resumeRepository.findById(RESUME_ID)).thenReturn(Optional.of(resume));
            lockAcquired();
            doThrow(new DataIntegrityViolationException("resume fk violation",
                    new SQLException("violates foreign key constraint on resume", "23503")))
                    .when(persistenceService).recordAnalysisResult(eq(MESSAGE_ID), any(), any());

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack);

            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("非重复/已删的完整性异常（22001 列超长）：重抛，不 ACK，避免被误判为已处理")
        void nonSkippableIntegrityViolationShouldRethrow() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ResumeEntity resume = new ResumeEntity();
            resume.setId(RESUME_ID);
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(true);
            when(gradingService.analyzeResume(CONTENT)).thenReturn(analysis());
            when(resumeRepository.findById(RESUME_ID)).thenReturn(Optional.of(resume));
            lockAcquired();
            doThrow(new DataIntegrityViolationException("value too long",
                    new SQLException("value too long for type character varying(64)", "22001")))
                    .when(persistenceService).recordAnalysisResult(eq(MESSAGE_ID), any(), any());

            assertThatThrownBy(() -> consumer.onAnalyzeTask(
                    new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack))
                    .isInstanceOf(DataIntegrityViolationException.class);

            verify(ack, never()).acknowledge();
        }

        @Test
        @DisplayName("业务失败：重抛异常，交给 DefaultErrorHandler 重试→DLQ")
        void businessFailureShouldRethrow() {
            Acknowledgment ack = mock(Acknowledgment.class);
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(true);
            lockAcquired();
            doThrow(new RuntimeException("LLM 不可用")).when(gradingService).analyzeResume(CONTENT);

            assertThatThrownBy(() -> consumer.onAnalyzeTask(
                    new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("LLM 不可用");

            verify(ack, never()).acknowledge();
        }

        @Test
        @DisplayName("Redis 不可用时降级为无锁处理：正常分析落库，幂等仍由 DB 唯一约束兜底")
        void redisDownShouldFallbackToUnlockedProcessing() {
            Acknowledgment ack = mock(Acknowledgment.class);
            ResumeEntity resume = new ResumeEntity();
            resume.setId(RESUME_ID);
            ResumeAnalysisResponse analysis = analysis();
            when(dedupRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
            when(resumeRepository.existsById(RESUME_ID)).thenReturn(true);
            when(redisService.tryLock(anyString(), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenThrow(new RuntimeException("Redis 连接失败"));
            when(gradingService.analyzeResume(CONTENT)).thenReturn(analysis);
            when(resumeRepository.findById(RESUME_ID)).thenReturn(Optional.of(resume));

            consumer.onAnalyzeTask(new ResumeAnalyzeTask(MESSAGE_ID, RESUME_ID, CONTENT), 1, ack);

            verify(persistenceService).recordAnalysisResult(MESSAGE_ID, resume, analysis);
            verify(ack).acknowledge();
        }
    }
}
