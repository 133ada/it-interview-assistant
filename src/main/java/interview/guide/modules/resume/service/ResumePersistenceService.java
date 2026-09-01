package interview.guide.modules.resume.service;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.file.FileHashService;
import interview.guide.infrastructure.mapper.ResumeMapper;
import interview.guide.modules.interview.model.ResumeAnalysisResponse;
import interview.guide.modules.resume.model.ResumeAnalyzeTask;
import interview.guide.modules.resume.model.ResumeAnalysisEntity;
import interview.guide.modules.resume.model.ResumeEntity;
import interview.guide.modules.resume.model.ResumeMessageDedupEntity;
import interview.guide.modules.resume.repository.ResumeAnalysisRepository;
import interview.guide.modules.resume.repository.ResumeMessageDedupRepository;
import interview.guide.modules.resume.repository.ResumeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;

/**
 * 简历持久化服务
 * 简历和评测结果的持久化，简历删除时删除所有关联数据
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ResumePersistenceService {

    private final ResumeRepository resumeRepository;
    private final ResumeAnalysisRepository analysisRepository;
    private final ObjectMapper objectMapper;
    private final ResumeMapper resumeMapper;
    private final FileHashService fileHashService;
    private final ResumeAnalyzeOutboxService outboxService;
    private final ResumeMessageDedupRepository dedupRepository;

    /**
     * 检查简历是否已存在（基于文件内容hash）
     *
     * @param file 上传的文件
     * @return 如果存在返回已有的简历实体，否则返回空
     */
    public Optional<ResumeEntity> findExistingResume(MultipartFile file) {
        try {
            String fileHash = fileHashService.calculateHash(file);
            Optional<ResumeEntity> existing = resumeRepository.findByFileHash(fileHash);

            if (existing.isPresent()) {
                log.info("检测到重复简历: hash={}", fileHash);
                ResumeEntity resume = existing.get();
                resume.incrementAccessCount();
                resumeRepository.save(resume);
            }

            return existing;
        } catch (Exception e) {
            log.error("检查简历重复时出错: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * 保存新简历
     */
    @Transactional(rollbackFor = Exception.class)
    public ResumeEntity saveResume(MultipartFile file, String resumeText,
                                   String storageKey, String storageUrl) {
        try {
            String fileHash = fileHashService.calculateHash(file);

            ResumeEntity resume = new ResumeEntity();
            resume.setFileHash(fileHash);
            resume.setOriginalFilename(file.getOriginalFilename());
            resume.setFileSize(file.getSize());
            resume.setContentType(file.getContentType());
            resume.setStorageKey(storageKey);
            resume.setStorageUrl(storageUrl);
            resume.setResumeText(resumeText);

            ResumeEntity saved = resumeRepository.save(resume);
            log.info("简历已保存: id={}, hash={}", saved.getId(), fileHash);

            // 同一事务内写入 Outbox 任务：简历入库与待投递任务原子提交，
            outboxService.createTask(saved.getId(), resumeText);

            return saved;
        } catch (BusinessException e) {
            // 原样传递业务异常（如 Outbox 写入失败），避免被包装成 RESUME_UPLOAD_FAILED
            throw e;
        } catch (Exception e) {
            log.error("保存简历失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.RESUME_UPLOAD_FAILED, "保存简历失败");
        }
    }

    /**
     * 保存简历评测结果
     */
    @Transactional(rollbackFor = Exception.class)
    public ResumeAnalysisEntity saveAnalysis(ResumeEntity resume, ResumeAnalysisResponse analysis) {
        try {
            ResumeAnalysisEntity entity = buildAnalysisEntity(resume, analysis);

            ResumeAnalysisEntity saved = analysisRepository.save(entity);
            log.info("简历评测结果已保存: analysisId={}, resumeId={}, score={}",
                    saved.getId(), resume.getId(), analysis.overallScore());

            return saved;
        } catch (JacksonException e) {
            log.error("序列化评测结果失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.RESUME_ANALYSIS_FAILED, "保存评测结果失败");
        }
    }

    /**
     * 构建分析实体（供 {@link #saveAnalysis} / {@link #recordAnalysisResult} 复用，
     * 避免同类内对 @Transactional 方法的自调用，保证事务边界清晰）
     */
    private ResumeAnalysisEntity buildAnalysisEntity(ResumeEntity resume, ResumeAnalysisResponse analysis)
            throws JacksonException {
        // 使用 MapStruct 映射基础字段
        ResumeAnalysisEntity entity = resumeMapper.toAnalysisEntity(analysis);
        entity.setResume(resume);

        // JSON 字段需要手动序列化
        entity.setStrengthsJson(objectMapper.writeValueAsString(analysis.strengths()));
        entity.setSuggestionsJson(objectMapper.writeValueAsString(analysis.suggestions()));
        return entity;
    }

    /**
     * 落库分析结果（同一事务内原子提交：幂等行 + 分析记录 + 状态 COMPLETED）。
     * <p>
     * 幂等行插入会触发 messageId 唯一约束：并发重复消息在此抛
     * {@code DataIntegrityViolationException}，由调用方（消费者）判定为"已处理，跳过"。
     * 该异常<b>必须传播出本事务</b>（Spring 已将该事务标记 rollback-only），不可在事务内吞掉。
     *
     * @param messageId 消息唯一 ID（幂等键）
     * @param resume    简历实体（detached，仅用主键关联）
     * @param analysis  AI 分析结果
     */
    @Transactional(rollbackFor = Exception.class)
    public void recordAnalysisResult(String messageId, ResumeEntity resume, ResumeAnalysisResponse analysis) {
        // 1. 幂等行：flush 立即触发唯一约束；并发重复在此抛 DataIntegrityViolationException
        ResumeMessageDedupEntity dedup = ResumeMessageDedupEntity.builder()
                .messageId(messageId)
                .resumeId(resume.getId())
                .build();
        dedupRepository.save(dedup);
        dedupRepository.flush();

        // 2. 分析结果落库
        analysisRepository.save(buildAnalysisEntity(resume, analysis));

        // 3. 守卫式置 COMPLETED（仅 PROCESSING → COMPLETED；被 reanalyze 重置为 PENDING 时不覆盖）
        resumeRepository.updateAnalyzeStatus(resume.getId(), AsyncTaskStatus.COMPLETED, null, AsyncTaskStatus.PROCESSING);
    }

    /**
     * 标记分析中（守卫式：仅 PENDING → PROCESSING，避免覆盖已完成的其它状态）
     */
    @Transactional(rollbackFor = Exception.class)
    public void markProcessing(Long resumeId) {
        resumeRepository.updateAnalyzeStatus(resumeId, AsyncTaskStatus.PROCESSING, null, AsyncTaskStatus.PENDING);
    }

    @Transactional(rollbackFor = Exception.class)
    public void saveResumeEvaluateStateFAILED(ResumeAnalyzeTask task, Exception ex) {
        String error = truncate("简历分析失败: " + (ex != null ? ex.getMessage() : "unknown"));

        // 加行锁（悲观锁的实现）
        ResumeEntity resume = resumeRepository.findByResumeIdForUpdate(task.resumeId())
                .orElseThrow(() ->
                        new BusinessException(
                                ErrorCode.RESUME_NOT_FOUND.getCode(),
                                ErrorCode.RESUME_NOT_FOUND.getMessage()
                        )
                );
        resume.setAnalyzeStatus(AsyncTaskStatus.FAILED);
        resume.setAnalyzeError(error);
        resumeRepository.save(resume);

        log.warn(
                "简历分析已标记最终失败: resumeId={}, error={}",
                task.resumeId(),
                error
        );
    }

    /**
     * 获取简历的最新评测结果
     */
    public Optional<ResumeAnalysisEntity> getLatestAnalysis(Long resumeId) {
        return Optional.ofNullable(analysisRepository.findFirstByResumeIdOrderByAnalyzedAtDesc(resumeId));
    }

    /**
     * 获取简历的最新评测结果（返回DTO）
     */
    public Optional<ResumeAnalysisResponse> getLatestAnalysisAsDTO(Long resumeId) {
        return getLatestAnalysis(resumeId).map(this::entityToDTO);
    }

    /**
     * 获取所有简历列表
     */
    public List<ResumeEntity> findAllResumes() {
        return resumeRepository.findAll();
    }

    /**
     * 获取简历的所有评测记录
     */
    public List<ResumeAnalysisEntity> findAnalysesByResumeId(Long resumeId) {
        return analysisRepository.findByResumeIdOrderByAnalyzedAtDesc(resumeId);
    }

    /**
     * 将实体转换为DTO
     */
    public ResumeAnalysisResponse entityToDTO(ResumeAnalysisEntity entity) {
        try {
            List<String> strengths = objectMapper.readValue(
                    entity.getStrengthsJson() != null ? entity.getStrengthsJson() : "[]",
                    new TypeReference<>() {
                    }
            );

            List<ResumeAnalysisResponse.Suggestion> suggestions = objectMapper.readValue(
                    entity.getSuggestionsJson() != null ? entity.getSuggestionsJson() : "[]",
                    new TypeReference<>() {
                    }
            );

            return new ResumeAnalysisResponse(
                    entity.getOverallScore(),
                    resumeMapper.toScoreDetail(entity),  // 使用MapStruct自动映射
                    entity.getSummary(),
                    strengths,
                    suggestions,
                    entity.getResume().getResumeText()
            );
        } catch (JacksonException e) {
            log.error("反序列化评测结果失败: {}", e.getMessage());
            throw new BusinessException(ErrorCode.RESUME_ANALYSIS_FAILED, "获取评测结果失败");
        }
    }

    /**
     * 根据ID获取简历
     */
    public Optional<ResumeEntity> findById(Long id) {
        return resumeRepository.findById(id);
    }

    /**
     * 删除简历及其所有关联数据
     * 包括：简历分析记录、面试会话（会自动删除面试答案）
     */
    @Transactional(rollbackFor = Exception.class)
    public void deleteResume(Long id) {
        Optional<ResumeEntity> resumeOpt = resumeRepository.findById(id);
        if (resumeOpt.isEmpty()) {
            throw new BusinessException(ErrorCode.RESUME_NOT_FOUND);
        }

        ResumeEntity resume = resumeOpt.get();

        // 0. 清理异步链路孤儿数据（Outbox 待投递任务 + 消息幂等记录），防止中继残留重投
        outboxService.deleteByResumeId(id);
        dedupRepository.deleteByResumeId(id);

        // 1. 删除所有简历分析记录
        List<ResumeAnalysisEntity> analyses = analysisRepository.findByResumeIdOrderByAnalyzedAtDesc(id);
        if (!analyses.isEmpty()) {
            analysisRepository.deleteAll(analyses);
            log.info("已删除 {} 条简历分析记录", analyses.size());
        }

        // 2. 删除简历实体（面试会话会在服务层删除）
        resumeRepository.delete(resume);
        log.info("简历已删除: id={}, filename={}", id, resume.getOriginalFilename());
    }

    private String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() > 500 ? error.substring(0, 500) : error;
    }
}
