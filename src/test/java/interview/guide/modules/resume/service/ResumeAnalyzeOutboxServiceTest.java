package interview.guide.modules.resume.service;

import interview.guide.modules.resume.model.OutboxStatus;
import interview.guide.modules.resume.model.ResumeAnalyzeOutboxEntity;
import interview.guide.modules.resume.repository.ResumeAnalyzeOutboxRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link ResumeAnalyzeOutboxService} 单元测试
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ResumeAnalyzeOutboxService 测试")
class ResumeAnalyzeOutboxServiceTest {

    @Mock
    private ResumeAnalyzeOutboxRepository outboxRepository;

    @InjectMocks
    private ResumeAnalyzeOutboxService outboxService;

    @Nested
    @DisplayName("createTask()")
    class CreateTaskTests {

        @Test
        @DisplayName("生成非空 UUID messageId，插入 PENDING 待投递行，保存简历内容快照")
        void createTaskShouldInsertPendingRowWithUuid() {
            Long resumeId = 100L;
            String content = "简历文本快照";

            String messageId = outboxService.createTask(resumeId, content);

            assertThat(messageId).isNotBlank();

            ArgumentCaptor<ResumeAnalyzeOutboxEntity> captor = ArgumentCaptor.forClass(ResumeAnalyzeOutboxEntity.class);
            verify(outboxRepository).save(captor.capture());
            ResumeAnalyzeOutboxEntity saved = captor.getValue();
            assertThat(saved.getMessageId()).isEqualTo(messageId);
            assertThat(saved.getResumeId()).isEqualTo(resumeId);
            assertThat(saved.getContent()).isEqualTo(content);
            assertThat(saved.getStatus()).isEqualTo(OutboxStatus.PENDING);
            assertThat(saved.getRetryCount()).isZero();
        }
    }

    @Nested
    @DisplayName("守卫式状态迁移")
    class GuardedTransitionTests {

        @Test
        @DisplayName("markSent 仅允许 PENDING→SENT，受影响 1 行时返回 true")
        void markSentShouldOnlyTransitionFromPending() {
            when(outboxRepository.updateStatusIf(1L, OutboxStatus.SENT, null, OutboxStatus.PENDING)).thenReturn(1);

            boolean result = outboxService.markSent(1L);

            assertThat(result).isTrue();
            verify(outboxRepository).updateStatusIf(1L, OutboxStatus.SENT, null, OutboxStatus.PENDING);
        }

        @Test
        @DisplayName("状态已被其它实例迁移时（0 行影响），markDlq 返回 false")
        void markDlqShouldReturnFalseWhenAlreadyMigrated() {
            when(outboxRepository.updateStatusIf(1L, OutboxStatus.DLQ, "error", OutboxStatus.PENDING)).thenReturn(0);

            boolean result = outboxService.markDlq(1L, "error");

            assertThat(result).isFalse();
        }

        @Test
        @DisplayName("markFailed 仅允许 PENDING→FAILED")
        void markFailedShouldOnlyTransitionFromPending() {
            when(outboxRepository.updateStatusIf(1L, OutboxStatus.FAILED, "boom", OutboxStatus.PENDING)).thenReturn(1);

            boolean result = outboxService.markFailed(1L, "boom");

            assertThat(result).isTrue();
        }
    }
}
