package interview.guide.modules.interview.service;

import interview.guide.common.constant.CommonConstants.InterviewDefaults;
import interview.guide.common.ai.LlmProviderRegistry;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.InterviewSessionCache;
import interview.guide.infrastructure.redis.InterviewSessionCache.CachedSession;
import interview.guide.modules.interview.listener.EvaluateStreamProducer;
import interview.guide.modules.interview.model.CreateInterviewRequest;
import interview.guide.modules.interview.model.CurrentQuestionResponse;
import interview.guide.modules.interview.model.HistoricalQuestion;
import interview.guide.modules.interview.model.InterviewAnswerEntity;
import interview.guide.modules.interview.model.InterviewQuestionDTO;
import interview.guide.modules.interview.model.InterviewReportDTO;
import interview.guide.modules.interview.model.InterviewSessionDTO;
import interview.guide.modules.interview.model.InterviewSessionEntity;
import interview.guide.modules.interview.model.SubmitAnswerRequest;
import interview.guide.modules.interview.model.SubmitAnswerResponse;
import interview.guide.modules.interview.model.InterviewSessionDTO.SessionStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

// ════════════════════════════════════════════════════════════════════════════════
// ★ 架构定位：本类是"编排层（Orchestration Layer）"
// ════════════════════════════════════════════════════════════════════════════════
//
// 整体分层：
//   Controller
//       ↓
//   InterviewSessionService（本类）── 编排 Redis + MySQL + LLM + MQ，无 @Transactional
//           ↓                      ↓
//   InterviewSessionCache    InterviewPersistenceService（有 @Transactional，管理 MySQL 事务）
//   （Redis 操作）            （JPA 操作）
//
// ┌─ 为什么本类没有 @Transactional？─────────────────────────────────────────────┐
// │  Spring 的 @Transactional 管理的是"数据库连接"事务（JDBC Connection）。      │
// │  本类同时操作 Redis 和 MySQL，而 Redis 不参与 Spring 的事务管理器（无 XA）。  │
// │  若给本类加 @Transactional，只能保证 MySQL 的原子性，Redis 操作不受保护。     │
// │  因此本类选择：由下层 InterviewPersistenceService 的各方法各自保证 MySQL      │
// │  的原子性，本类只负责编排顺序和处理降级逻辑。                                 │
// └───────────────────────────────────────────────────────────────────────────────┘
//
// ┌─ Redis 与 MySQL 的一致性策略（标准 Cache-Aside 模式）────────────────────────┐
// │                                                                          │
// │  【写入顺序】：先写 MySQL（真相源），成功后删除缓存（缓存失效，而非更新缓存）       │
// │    MySQL 写入失败 → 异常直接传播，缓存不删，两边一致。                         │
// │    Redis 删除失败 → 残留旧值会在 TTL 内被丢弃，MySQL 始终是唯一真相源，          │
// │                    下次读取可能短暂命中旧值，时间窗极小，可接受。               │
// │    为什么删除而不是更新缓存？删除后下一次读取必定从 MySQL 重新加载，              │
// │    不存在"DB 已更新、缓存仍旧值"的中间态，最终一致性更稳。                       │
// │                                                                          │
// │  【读取顺序】：先读 Redis（L1缓存），未命中则读 MySQL 并回填 Redis              │
// │      1. 读 Redis → 命中 → 返回（刷新 TTL）                                  │
// │      2. 读 Redis → 未命中 → 读 MySQL → 写入 Redis → 返回                    │
// └───────────────────────────────────────────────────────────────────────────────┘
//
// ┌─ 本类相关的事务隔离级别说明 ─────────────────────────────────────────────────┐
// │  本类自身无事务，下层 InterviewPersistenceService 的方法使用：                │
// │    @Transactional(rollbackFor = Exception.class)                        │
// │    隔离级别：Isolation.DEFAULT（沿用数据库默认值）                            │
// │      MySQL InnoDB  → REPEATABLE_READ                                    │
// │      PostgreSQL    → READ_COMMITTED                                     │
// │  本类通过多次独立调用 persistenceService 的方法，每次调用是独立事务，             │
// │  方法之间不共享同一个数据库连接，因此无跨方法事务保证。                            │
// └─────────────────────────────────────────────────────────────────────────┘

/**
 * 面试会话管理服务
 * 管理面试会话的生命周期，使用 Redis 缓存会话状态
 */

@Slf4j
@Service
@RequiredArgsConstructor
// 构造器注入（单例 Bean）（Lombok @RequiredArgsConstructor），本类无 @Transactional：编排层，事务由下层 PersistenceService 管理。
public class InterviewSessionService {

    private final InterviewQuestionService questionService;
    // 负责调用 LLM 生成面试题目，内部调用 Spring AI 的 ChatClient。

    private final AnswerEvaluationService evaluationService;
    // 负责调用 LLM 对答案进行综合评估，生成面试报告。

    private final InterviewPersistenceService persistenceService;
    // MySQL 持久化服务，所有数据库操作通过此服务完成，其方法带 @Transactional。

    private final InterviewSessionCache sessionCache;
    // Redis 缓存操作封装，负责会话数据的存取和 TTL 管理。

    private final ObjectMapper objectMapper;
    // Jackson 序列化工具，线程安全，可在多线程环境中共享使用。

    private final EvaluateStreamProducer evaluateStreamProducer;
    // Redis Stream 消息生产者，向消费者组发送异步评估任务。

    private final LlmProviderRegistry llmProviderRegistry;
    // LLM 提供商注册表，根据 provider 名称获取对应的 ChatClient 实例。

    // ════════════════════════════════════════════════════════════════════════════
    // 方法一：createSession —— 创建新的面试会话
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 创建新的面试会话
     * 注意：如果已有未完成的会话，不会创建新的，而是返回现有会话
     * 前端应该先调用 findUnfinishedSession 检查，或者使用 forceCreate 参数强制创建
     */
    public InterviewSessionDTO createSession(CreateInterviewRequest request) {
        if (request.resumeId() != null && !Boolean.TRUE.equals(request.forceCreate())) {
            Optional<InterviewSessionDTO> unfinishedOpt = findUnfinishedSession(request.resumeId());
            if (unfinishedOpt.isPresent()) {
                log.info("检测到未完成的面试会话，返回现有会话: resumeId={}, sessionId={}",
                        request.resumeId(), unfinishedOpt.get().sessionId());
                return unfinishedOpt.get();
            }
        }

        String sessionId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        String skillId = request.skillId() != null ? request.skillId() : InterviewDefaults.SKILL_ID;
        String difficulty = request.difficulty() != null ? request.difficulty() : InterviewDefaults.DIFFICULTY;

        log.info("创建新面试会话: {}, skill: {}, difficulty: {}, questionCount: {}, resumeId: {}",
                sessionId, skillId, difficulty, request.questionCount(), request.resumeId());

        List<HistoricalQuestion> historicalQuestions =
                persistenceService.getHistoricalQuestions(skillId, request.resumeId());

        List<InterviewQuestionDTO> questions = questionService.generateQuestionsBySkill(
                request.llmProvider(),
                skillId,
                difficulty,
                request.resumeText(),
                request.questionCount(),
                historicalQuestions,
                request.customCategories(),
                request.jdText()
        );

        // Cache-Aside 写入：只写 MySQL（真相源），不预写 Redis 缓存。
        // 首次读取会话时缓存未命中，会走"查 MySQL → 回填缓存"路径自动补上
        // （见 getOrRestoreSession / restoreSessionFromEntity）。
        persistenceService.saveSession(sessionId, request.resumeId(),
                questions.size(), questions, request.llmProvider(), skillId, difficulty);

        log.info("面试会话已持久化到 MySQL: sessionId={}", sessionId);

        return new InterviewSessionDTO(
                sessionId,
                request.resumeText() != null ? request.resumeText() : "",
                questions.size(),
                0,
                questions,
                SessionStatus.CREATED
        );
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法二：getSession —— 获取会话信息（Redis 优先，MySQL 兜底）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 获取会话信息（优先从缓存获取，缓存未命中则从数据库恢复）
     */
    public InterviewSessionDTO getSession(String sessionId) {
        // Cache-Aside 读出：先读 Redis，成功则直接返回。若 Redis 不存在，那么就去查询 MySQL。再回填 Redis
        Optional<CachedSession> cachedOpt = sessionCache.getSession(sessionId);
        if (cachedOpt.isPresent()) {
            // 缓存命中
            return toDTO(cachedOpt.get());
        }

        // Redis 未命中，从 MySQL 恢复并回填 Redis（Cache-Aside 写回）
        CachedSession restoredSession = restoreSessionFromDatabase(sessionId);
        // 读 MySQL → 将数据写入 Redis → 返回数据（详见 restoreSessionFromDatabase）
        // 回填 Redis 后，下次请求直接走 Redis，不再查 MySQL。

        if (restoredSession == null) {
            throw new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND);
            // MySQL 也没有该 session，说明 sessionId 无效或数据从未写入 MySQL
        }

        return toDTO(restoredSession);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法三：findUnfinishedSession —— 查找未完成的面试会话
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 查找并恢复未完成的面试会话
     */
    public Optional<InterviewSessionDTO> findUnfinishedSession(Long resumeId) {
        // ── 第一步：查 Redis 中是否有该 resumeId 的活跃 session ───────────────
        Optional<String> cachedSessionIdOpt = sessionCache.findUnfinishedSessionId(resumeId);

        if (cachedSessionIdOpt.isPresent()) {
            String sessionId = cachedSessionIdOpt.get();
            Optional<CachedSession> cachedOpt = sessionCache.getSession(sessionId);
            if (cachedOpt.isPresent()) {
                log.debug("从 Redis 缓存找到未完成会话: resumeId={}, sessionId={}", resumeId, sessionId);
                return Optional.of(toDTO(cachedOpt.get()));
                // Redis 双命中（resumeId → sessionId 映射 AND sessionId → session 数据）。
            }
            // 边界情况：resumeId → sessionId 映射存在，但 session 数据已过期（TTL 不同）。
        }

        // ── 第二步：MySQL 兜底查询 ────────────────────────────────────────────
        Optional<InterviewSessionEntity> entityOpt = persistenceService.findUnfinishedSession(resumeId);
        // SELECT * FROM interview_session WHERE resume_id = ? AND status IN ('CREATED','IN_PROGRESS')
        // ORDER BY created_at DESC LIMIT 1
        // persistenceService.findUnfinishedSession 无 @Transactional（只读查询，无事务）。

        // MySQL 中也没有未完成会话，确认不存在。
        if (entityOpt.isEmpty()) {
            return Optional.empty();
        }

        // MySQL 存在未完成会话，但 Redis 中不存在（缓存过期/重启），
        // 从 MySQL 实体恢复完整会话数据并写入 Redis 缓存。
        InterviewSessionEntity entity = entityOpt.get();
        CachedSession restoredSession = restoreSessionFromEntity(entity);

        if (restoredSession != null) {
            return Optional.of(toDTO(restoredSession));
        }
        // Redis / MySQL 均未找到未完成会话，返回空。
        // 等基础设施故障会向上传播到 GlobalExceptionHandler，避免被误判为"会话不存在"。
        return Optional.empty();
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法四：findUnfinishedSessionOrThrow —— 查找未完成会话，不存在则抛异常
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 查找并恢复未完成的面试会话，如果不存在则抛出异常
     */
    public InterviewSessionDTO findUnfinishedSessionOrThrow(Long resumeId) {
        return findUnfinishedSession(resumeId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND, "未找到未完成的面试会话"));
        // Optional.orElseThrow()：语义清晰的"必须存在"断言，Java 8+ 推荐写法。
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法五：restoreSessionFromDatabase —— 从 MySQL 恢复会话到 Redis（私有）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 从数据库恢复会话并缓存到 Redis（此方法要依赖于 私有方法 restoreSessionFromEntity 的实现）
     */
    private CachedSession restoreSessionFromDatabase(String sessionId) {
        Optional<InterviewSessionEntity> entityOpt = persistenceService.findBySessionId(sessionId);
        // 读 MySQL，persistenceService.findBySessionId 无 @Transactional（只读，无事务）。

        return entityOpt.map(this::restoreSessionFromEntity).orElse(null);
        // 若 MySQL 有记录，调用 restoreSessionFromEntity 恢复并写入 Redis，返回 CachedSession。
        // 若 MySQL 无记录，返回 null（调用方据此判断 session 不存在）。
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法六：restoreSessionFromEntity —— 从实体恢复会话并写入 Redis（私有）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 从实体恢复会话 并 缓存到 Redis 这里也即 Cache - aside 中的 读操作的 最后一步
     */
    private CachedSession restoreSessionFromEntity(InterviewSessionEntity entity) {
        // 反序列化题目列表
        List<InterviewQuestionDTO> questions = objectMapper.readValue(
                entity.getQuestionsJson(),
                new TypeReference<>() {
                }
        );

        // 恢复已保存的答案，填充到题目列表
        List<InterviewAnswerEntity> answers = persistenceService.findAnswersBySessionId(entity.getSessionId());
        for (InterviewAnswerEntity answer : answers) {
            int index = answer.getQuestionIndex();
            if (index >= 0 && index < questions.size()) {
                questions.set(index, questions.get(index).withAnswer(answer.getUserAnswer()));
            }
        }

        // 将恢复的数据写入 Redis（Cache-Aside 写回）
        SessionStatus status = convertStatus(entity.getStatus());
        sessionCache.saveSession(
                entity.getSessionId(),
                entity.getResume() != null ? entity.getResume().getResumeText() : "",
                entity.getResume() != null ? entity.getResume().getId() : null,
                questions,
                entity.getCurrentQuestionIndex(),
                status
        );

        log.info("从数据库恢复会话到 Redis: sessionId={}, currentIndex={}, status={}",
                entity.getSessionId(), entity.getCurrentQuestionIndex(), entity.getStatus());

        // 从 Redis 读回已写入的数据确认一致性后返回
        return sessionCache.getSession(entity.getSessionId()).orElse(null);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法七：convertStatus —— 状态枚举转换（私有工具方法）
    // ════════════════════════════════════════════════════════════════════════════

    private SessionStatus convertStatus(InterviewSessionEntity.SessionStatus status) {
        return switch (status) {
            case CREATED -> SessionStatus.CREATED;
            case IN_PROGRESS -> SessionStatus.IN_PROGRESS;
            case COMPLETED -> SessionStatus.COMPLETED;
            case EVALUATED -> SessionStatus.EVALUATED;
        };
        // switch 表达式（Java 14+），穷举所有枚举值，编译器强制处理所有分支。
        // 若未来 SessionStatus 增加新枚举值而此处未同步，编译期即报错，比 if-else 更安全。
        // 此处做了两层枚举的映射（Entity 层 → DTO 层），体现了分层隔离：
        // Entity 层枚举用于数据库映射，DTO 层枚举用于接口响应，两者可独立演化。
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法八：getCurrentQuestionResponse —— 获取当前题目响应（含完成标志）
    // ════════════════════════════════════════════════════════════════════════════

    public CurrentQuestionResponse getCurrentQuestionResponse(String sessionId) {
        InterviewQuestionDTO question = getCurrentQuestion(sessionId);
        // 委托给 getCurrentQuestion，复用其 Redis 读取 + 状态更新逻辑。

        if (question == null) {
            return new CurrentQuestionResponse(true, "所有问题已回答完毕", null);
        }
        return new CurrentQuestionResponse(false, null, question);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法九：getCurrentQuestion —— 获取当前题目（核心读方法）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 获取当前问题
     */
    public InterviewQuestionDTO getCurrentQuestion(String sessionId) {
        CachedSession session = getOrRestoreSession(sessionId);
        // Redis 优先，MySQL 兜底恢复。详见 getOrRestoreSession。

        List<InterviewQuestionDTO> questions = session.getQuestions(objectMapper);
        // 从 CachedSession 中反序列化题目列表（内部缓存在 Redis 中的 JSON 字符串）。

        if (session.getCurrentIndex() >= questions.size()) {
            return null;
            // 所有问题已回答完（currentIndex 已越界），返回 null。
            // 调用方（getCurrentQuestionResponse）据此判断面试是否完成。
        }

        // ── 首次获取题目时，将状态从 CREATED 推进为 IN_PROGRESS ──────────────────
        if (session.getStatus() == SessionStatus.CREATED) {

            // Cache-Aside 写入：先 MySQL（status → IN_PROGRESS），成功后删除缓存。
            // 下一次读取时缓存未命中，自动从 MySQL 重新加载最新状态并回填。
            persistenceService.updateSessionStatus(sessionId,
                    InterviewSessionEntity.SessionStatus.IN_PROGRESS);

            sessionCache.deleteSession(sessionId);

            session.setStatus(SessionStatus.IN_PROGRESS);
            // 同步内存中的状态，确保当前请求内后续逻辑看到最新值。
        }
        // 返回当前索引对应的题目。
        return questions.get(session.getCurrentIndex());
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十：submitAnswer —— 提交答案并进入下一题（核心写方法）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 提交答案（并进入下一题）
     * 如果是最后一题，自动触发异步评估
     */
    public SubmitAnswerResponse submitAnswer(SubmitAnswerRequest request) {

        // ── 第一步：从 Redis 读取会话（Cache-Aside 读，MySQL 兜底）──────────────────
        CachedSession session = getOrRestoreSession(request.sessionId());
        List<InterviewQuestionDTO> questions = session.getQuestions(objectMapper);

        int index = request.questionIndex();
        if (index < 0 || index >= questions.size()) {
            throw new BusinessException(ErrorCode.INTERVIEW_QUESTION_NOT_FOUND, "无效的问题索引: " + index);
        }

        // ── 第二步：在内存中计算好所有需要更新的数据 ─────────────────────────────────
        // 所有计算在写入任何存储之前完成，确保写入时数据已完整准备好。
        InterviewQuestionDTO question = questions.get(index);
        InterviewQuestionDTO answeredQuestion = question.withAnswer(request.answer());
        questions.set(index, answeredQuestion);

        int newIndex = index + 1;
        boolean hasNextQuestion = newIndex < questions.size();
        InterviewQuestionDTO nextQuestion = hasNextQuestion ? questions.get(newIndex) : null;
        SessionStatus newStatus = hasNextQuestion ? SessionStatus.IN_PROGRESS : SessionStatus.COMPLETED;
        InterviewSessionEntity.SessionStatus newDbStatus = hasNextQuestion
                ? InterviewSessionEntity.SessionStatus.IN_PROGRESS
                : InterviewSessionEntity.SessionStatus.COMPLETED;


        // ────────── 第三步：写入 MySQL ─────────────────────────────
        persistenceService.submitAnswer(
                request.sessionId(), index,
                question.question(), question.category(),
                request.answer(), newIndex, newDbStatus
        );

        if (!hasNextQuestion) {
            persistenceService.updateEvaluateStatus(request.sessionId(), AsyncTaskStatus.PENDING, null);
            // MySQL 中 evaluateStatus=PENDING 是消费者判断任务的依据，必须先于 MQ 投递写入。
            // 若此步失败：抛异常，MQ 消息不会发送，不会触发评估，状态一致。

            evaluateStreamProducer.sendEvaluateTask(request.sessionId());
            // 全部（包括重试投递）失败则保持 PENDING 状态，由定时补偿任务扫描并重新投递，保证最终一致性。

            log.info("会话 {} 已完成所有问题，评估任务已入队", request.sessionId());
        }

        log.info("MySQL 写入完成: sessionId={}, questionIndex={}, newIndex={}",
                request.sessionId(), index, newIndex);

        // ── 第四步：删除 Redis 缓存（Cache-Aside 写失效）────────────────────────────
        // MySQL 已写入最新数据（答案、索引、状态），删除缓存让下一次读取重新从 MySQL
        // 加载并回填。相比"直接更新缓存"，删除避免了"DB 已更新、缓存仍旧值"的中间态
        try {
            sessionCache.deleteSession(request.sessionId());
        } catch (Exception e) {
            // Redis 删除失败：仅打日志，不抛异常，不影响整体流程。
            // 残留的旧值缓存会在 TTL 内被丢弃，MySQL 始终是唯一真相源。
            log.warn("Redis 缓存删除失败，但 MySQL 已持久化，下次读取时将自动回填: sessionId={}, error={}",
                    request.sessionId(), e.getMessage());
        }

        log.info("会话 {} 提交答案: 问题{}, 剩余{}题",
                request.sessionId(), index, questions.size() - newIndex);

        return new SubmitAnswerResponse(hasNextQuestion, nextQuestion, newIndex, questions.size());
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十一：saveAnswer —— 暂存答案（不进入下一题）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 暂存答案（不进入下一题）
     */
    public void saveAnswer(SubmitAnswerRequest request) {
        CachedSession session = getOrRestoreSession(request.sessionId());
        List<InterviewQuestionDTO> questions = session.getQuestions(objectMapper);

        int index = request.questionIndex();
        if (index < 0 || index >= questions.size()) {
            throw new BusinessException(ErrorCode.INTERVIEW_QUESTION_NOT_FOUND, "无效的问题索引: " + index);
        }

        InterviewQuestionDTO question = questions.get(index);
        InterviewQuestionDTO answeredQuestion = question.withAnswer(request.answer());
        questions.set(index, answeredQuestion);

        // Cache-Aside 写入模式：先写 MySQL（真相源），成功后删除 Redis 缓存。
        // saveAnswerDraft 内部一个事务原子执行 saveAnswer + 可选 status 推进，
        // MySQL 失败 → 异常向上传播，缓存不删除，两边均无变更，天然一致。
        persistenceService.saveAnswerDraft(
                request.sessionId(), index,
                question.question(), question.category(),
                request.answer()
        );

        // MySQL 成功后删除缓存，让下一次读取重新加载最新答案。
        // Redis 删除失败仅影响缓存命中率（残留旧值会在 TTL 内被丢弃），不影响数据完整性。
        sessionCache.deleteSession(request.sessionId());

        log.info("会话 {} 暂存答案: 问题{}", request.sessionId(), index);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十二：completeInterview —— 提前交卷
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 提前交卷（触发异步评估）
     */
    public void completeInterview(String sessionId) {
        CachedSession session = getOrRestoreSession(sessionId);

        // 幂等保护：已交卷或已评估的会话不可重复交卷。
        if (session.getStatus() == SessionStatus.COMPLETED || session.getStatus() == SessionStatus.EVALUATED) {
            throw new BusinessException(ErrorCode.INTERVIEW_ALREADY_COMPLETED);
            // 并发安全：两个线程同时到达此处，MySQL 层 completeInterview 的 UPDATE
            // 实际执行时后提交者无影响（状态已是 COMPLETED，幂等 SET），
            // MQ 侧 EvaluateStreamProducer 已改为指数退避重试，Consumer 需幂等消费兜底。
        }

        // completeInterview 内部一个事务原子执行 status=COMPLETED + evaluateStatus=PENDING，
        // MySQL 失败 → 异常向上传播，缓存不删除，两边均无变更，天然一致。
        persistenceService.completeInterview(sessionId);

        // Cache-Aside 写失效：删除缓存，下一次读取从 MySQL 重新加载 COMPLETED 状态。
        // Redis 删除失败仅影响缓存命中率（残留旧值会在 TTL 内被丢弃）。
        sessionCache.deleteSession(sessionId);

        // 发送评估任务到 Redis Stream（内部有指数退避重试，失败保持 PENDING）
        evaluateStreamProducer.sendEvaluateTask(sessionId);

        log.info("会话 {} 提前交卷，评估任务已入队", sessionId);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十三：getOrRestoreSession —— 获取或恢复会话（私有核心方法）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 获取或恢复会话（Redis 优先，MySQL 兜底）。
     * 每次命中刷新 TTL 以维持滑动过期，活跃会话不会因超时被驱逐。
     */
    private CachedSession getOrRestoreSession(String sessionId) {
        // ── 第一步：读 Redis 缓存 ─────────────────────────────────────────
        // 先刷新 TTL 再读取：即使 key 在 expire 与 get 之间过期（24h TTL 下概率极低），
        // expire 对不存在的 key 是安全空操作，get 返回 null 后会走 MySQL 兜底回填。
        sessionCache.refreshSessionTTL(sessionId);
        Optional<CachedSession> cachedOpt = sessionCache.getSession(sessionId);
        if (cachedOpt.isPresent()) {
            return cachedOpt.get();
        }

        // ── 第二步：Redis 未命中，从 MySQL 恢复并回填 Redis ──────────────
        CachedSession restoredSession = restoreSessionFromDatabase(sessionId);
        if (restoredSession == null) {
            throw new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND);
        }
        return restoredSession;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十四：generateReport —— 生成评估报告（LLM 调用 + 持久化）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 生成评估报告（同步路径：用户主动请求查看报告）。
     * 用 SELECT ... FOR UPDATE 做 CAS 抢占，保证并发场景下只有一个线程执行 LLM 调用。
     */
    public InterviewReportDTO generateReport(String sessionId) {
        CachedSession session = getOrRestoreSession(sessionId);

        if (session.getStatus() != SessionStatus.COMPLETED
                && session.getStatus() != SessionStatus.EVALUATED) {
            throw new BusinessException(ErrorCode.INTERVIEW_NOT_COMPLETED, "面试尚未完成，无法生成报告");
        }

        // CAS 抢占评估权：SELECT ... FOR UPDATE 串行化，保证只有一个线程能成功抢占。
        // 若已被 Consumer 评估完毕或被其他同步请求抢占，CAS 返回 false。
        if (!persistenceService.tryClaimForEvaluation(sessionId)) {
            // 重新加载 MySQL 状态，区分"已完成"和"进行中"
            InterviewSessionEntity entity = persistenceService.findBySessionId(sessionId)
                    .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));
            if (entity.getStatus() == InterviewSessionEntity.SessionStatus.EVALUATED) {
                // 已被异步 Consumer 评估完毕：删除缓存让下次读取加载最新状态，
                // 再提示前端使用详情接口获取报告。
                sessionCache.deleteSession(sessionId);
                throw new BusinessException(ErrorCode.INTERVIEW_ALREADY_COMPLETED,
                        "报告已由系统自动生成，请刷新页面查看");
            }
            // 正在评估中（同步请求或 Consumer 正在处理）
            throw new BusinessException(ErrorCode.INTERVIEW_EVALUATION_FAILED,
                    "报告正在生成中，请稍后刷新页面");
        }

        log.info("生成面试报告: sessionId={}", sessionId);

        List<InterviewQuestionDTO> questions = session.getQuestions(objectMapper);

        InterviewSessionEntity entity = persistenceService.findBySessionId(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));
        ChatClient chatClient = llmProviderRegistry.getChatClientOrDefault(entity.getLlmProvider());

        // 调用 LLM（耗时 10~60 秒，期间不占用 DB 连接）
        InterviewReportDTO report = evaluationService.evaluateInterview(
                chatClient, sessionId, session.getResumeText(), questions);

        // Cache-Aside 写入：先写 MySQL（真相源），成功后删除 Redis 缓存。
        // saveReport 内部 @Transactional，status→EVALUATED + 报告落库原子完成。
        persistenceService.saveReport(sessionId, report);

        // 写失效：删除缓存，下一次读取从 MySQL 重新加载 EVALUATED 状态与报告。
        sessionCache.deleteSession(sessionId);

        return report;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十五：toDTO —— 将 CachedSession 转换为 InterviewSessionDTO（私有工具方法）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 将缓存会话转换为 DTO
     */
    private InterviewSessionDTO toDTO(CachedSession session) {
        List<InterviewQuestionDTO> questions = session.getQuestions(objectMapper);
        // 从 CachedSession 反序列化题目列表（每次调用都反序列化，存在性能开销）。
        // 优化方向：CachedSession 内部缓存已反序列化的 questions 列表，避免重复解析。

        return new InterviewSessionDTO(
                session.getSessionId(),
                session.getResumeText(),
                questions.size(),           // totalQuestions
                session.getCurrentIndex(),  // currentQuestionIndex
                questions,                  // 完整题目列表（含答案）
                session.getStatus()         // 当前状态
        );
        // InterviewSessionDTO 是不可变 Record 或含全参构造的 DTO，
        // 每次创建新实例，无共享状态，线程安全。
    }

}

