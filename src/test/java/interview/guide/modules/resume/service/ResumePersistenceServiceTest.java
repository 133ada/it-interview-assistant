package interview.guide.modules.resume.service;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.file.FileHashService;
import interview.guide.infrastructure.mapper.ResumeMapper;
import interview.guide.modules.interview.model.ResumeAnalysisResponse;
import interview.guide.modules.resume.model.ResumeAnalysisEntity;
import interview.guide.modules.resume.model.ResumeEntity;
import interview.guide.modules.resume.model.ResumeMessageDedupEntity;
import interview.guide.modules.resume.repository.ResumeAnalysisRepository;
import interview.guide.modules.resume.repository.ResumeMessageDedupRepository;
import interview.guide.modules.resume.repository.ResumeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.web.multipart.MultipartFile;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link ResumePersistenceService} 单元测试：幂等落库（recordAnalysisResult）与 Outbox 接入。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ResumePersistenceService 测试")
class ResumePersistenceServiceTest {

    @Mock
    private ResumeRepository resumeRepository;
    @Mock
    private ResumeAnalysisRepository analysisRepository;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private ResumeMapper resumeMapper;
    @Mock
    private FileHashService fileHashService;
    @Mock
    private ResumeAnalyzeOutboxService outboxService;
    @Mock
    private ResumeMessageDedupRepository dedupRepository;

    private ResumePersistenceService persistenceService;

    @BeforeEach
    void setUp() {
        persistenceService = new ResumePersistenceService(
                resumeRepository, analysisRepository, objectMapper, resumeMapper,
                fileHashService, outboxService, dedupRepository);
    }

    private ResumeEntity resume(Long id) {
        ResumeEntity resume = new ResumeEntity();
        resume.setId(id);
        return resume;
    }

    private ResumeAnalysisResponse analysis() {
        return new ResumeAnalysisResponse(
                85,
                new ResumeAnalysisResponse.ScoreDetail(20, 17, 20, 13, 15),
                "summary",
                List.of("优点"),
                List.of(new ResumeAnalysisResponse.Suggestion("内容", "高", "问题", "建议")),
                "content"
        );
    }

    @Nested
    @DisplayName("recordAnalysisResult()")
    class RecordAnalysisResultTests {

        @Test
        @DisplayName("同一事务顺序：幂等行 → flush → 分析落库 → 守卫式置 COMPLETED")
        void shouldCommitDedupAnalysisAndStatusInOrder() {
            ResumeEntity resume = resume(100L);
            ResumeAnalysisResponse analysis = analysis();
            when(resumeMapper.toAnalysisEntity(analysis)).thenReturn(new ResumeAnalysisEntity());
            when(objectMapper.writeValueAsString(any())).thenReturn("[]");
            when(resumeRepository.updateAnalyzeStatus(100L, AsyncTaskStatus.COMPLETED, null, AsyncTaskStatus.PROCESSING))
                    .thenReturn(1);

            persistenceService.recordAnalysisResult("m-1", resume, analysis);

            InOrder inOrder = inOrder(dedupRepository, analysisRepository, resumeRepository);
            inOrder.verify(dedupRepository).save(any(ResumeMessageDedupEntity.class));
            inOrder.verify(dedupRepository).flush();
            inOrder.verify(analysisRepository).save(any(ResumeAnalysisEntity.class));
            inOrder.verify(resumeRepository).updateAnalyzeStatus(
                    100L, AsyncTaskStatus.COMPLETED, isNull(), AsyncTaskStatus.PROCESSING);
        }

        @Test
        @DisplayName("幂等行唯一约束冲突：DataIntegrityViolationException 传播出事务，不吞掉")
        void duplicateKeyShouldPropagateOutOfTransaction() {
            ResumeEntity resume = resume(100L);
            when(dedupRepository.save(any(ResumeMessageDedupEntity.class))).thenReturn(null);
            doThrow(new DataIntegrityViolationException("duplicate key value violates unique constraint"))
                    .when(dedupRepository).flush();

            assertThatThrownBy(() -> persistenceService.recordAnalysisResult("m-1", resume, analysis()))
                    .isInstanceOf(DataIntegrityViolationException.class);

            verify(analysisRepository, never()).save(any());
        }
    }

    @Nested
    @DisplayName("saveResume()")
    class SaveResumeTests {

        @Test
        @DisplayName("简历入库后在同一调用链内创建 Outbox 任务")
        void shouldCreateOutboxTaskAfterSave() {
            MultipartFile file = mock(MultipartFile.class);
            when(fileHashService.calculateHash(file)).thenReturn("hash-1");
            ResumeEntity saved = resume(100L);
            when(resumeRepository.save(any(ResumeEntity.class))).thenReturn(saved);

            persistenceService.saveResume(file, "text", "file-key", "file-url");

            verify(outboxService).createTask(100L, "text");
        }

        @Test
        @DisplayName("Outbox 创建失败时原样抛业务异常，不被包装成 RESUME_UPLOAD_FAILED")
        void outboxFailureShouldPropagateBusinessException() {
            MultipartFile file = mock(MultipartFile.class);
            when(fileHashService.calculateHash(file)).thenReturn("hash-1");
            ResumeEntity saved = resume(100L);
            when(resumeRepository.save(any(ResumeEntity.class))).thenReturn(saved);
            doThrow(new BusinessException(ErrorCode.RESUME_ANALYSIS_FAILED, "outbox write failed"))
                    .when(outboxService).createTask(100L, "text");

            assertThatThrownBy(() -> persistenceService.saveResume(file, "text", "file-key", "file-url"))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(e -> {
                        BusinessException be = (BusinessException) e;
                        org.assertj.core.api.Assertions.assertThat(be.getCode())
                                .isEqualTo(ErrorCode.RESUME_ANALYSIS_FAILED.getCode());
                    });
        }
    }

    @Nested
    @DisplayName("deleteResume()")
    class DeleteResumeTests {

        @Test
        @DisplayName("删除简历时清理 Outbox 与幂等记录孤儿行")
        void shouldPurgeOrphanAsyncRows() {
            ResumeEntity resume = resume(100L);
            when(resumeRepository.findById(100L)).thenReturn(Optional.of(resume));
            when(analysisRepository.findByResumeIdOrderByAnalyzedAtDesc(100L)).thenReturn(List.of());

            persistenceService.deleteResume(100L);

            verify(outboxService).deleteByResumeId(100L);
            verify(dedupRepository).deleteByResumeId(100L);
            verify(resumeRepository).delete(resume);
        }
    }
}
