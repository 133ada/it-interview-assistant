package interview.guide.modules.interview.service;

import interview.guide.common.constant.CommonConstants.InterviewDefaults;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.modules.interview.model.HistoricalQuestion;
import interview.guide.modules.interview.model.InterviewAnswerEntity;
import interview.guide.modules.interview.model.InterviewQuestionDTO;
import interview.guide.modules.interview.model.InterviewReportDTO;
import interview.guide.modules.interview.model.InterviewSessionEntity;
import interview.guide.modules.interview.repository.InterviewAnswerRepository;
import interview.guide.modules.interview.repository.InterviewSessionRepository;
import interview.guide.modules.resume.model.ResumeEntity;
import interview.guide.modules.resume.repository.ResumeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.ArrayList;
// ════════════════════════════════════════════════════════════════════════════════
// ★ 核心概念速查：Spring 事务 & 数据库事务隔离级别
// ════════════════════════════════════════════════════════════════════════════════
//
// ┌─ 一、什么是数据库事务（ACID）─────────────────────────────────────────────────┐
// │  Atomicity  原子性：事务内所有操作要么全部成功，要么全部回滚，不允许部分提交。    │
// │  Consistency 一致性：事务执行前后，数据库从一个合法状态变迁到另一个合法状态。     │
// │  Isolation  隔离性：并发事务之间互不干扰，具体程度由隔离级别控制。               │
// │  Durability 持久性：事务提交后，数据永久写入磁盘，系统崩溃也不会丢失。           │
// └───────────────────────────────────────────────────────────────────────────────┘
//
// ┌─ 二、SQL 标准定义的四种隔离级别（由低到高）───────────────────────────────────┐
// │  级别                  脏读   不可重复读   幻读    说明                         │
// │  READ_UNCOMMITTED       ✓       ✓          ✓      可读到未提交数据，几乎不用    │
// │  READ_COMMITTED         ✗       ✓          ✓      只读已提交数据（PG/Oracle默认）│
// │  REPEATABLE_READ        ✗       ✗          ✓      同一事务内多次读结果一致      │
// │                                                    （MySQL InnoDB 默认）         │
// │  SERIALIZABLE           ✗       ✗          ✗      完全串行，最强但性能最差       │
// │  READ_UNCOMMITTED（读未提交） READ_COMMITTED（读已提交） REPEATABLE_READ（可重复读） SERIALIZABLE（串行化）│
// │  脏读：读到另一个事务尚未提交的修改。                                            │
// │  不可重复读：同一事务两次读同一行，结果不同（行被其他事务修改并提交）。            │
// │  幻读：同一事务两次范围查询，第二次多出或少了行（被其他事务插入/删除并提交）。     │
// └───────────────────────────────────────────────────────────────────────────────┘
//
// ┌─ 三、Spring 事务的工作机制 ────────────────────────────────────────────────────┐
// │  Spring 通过 AOP 动态代理拦截 @Transactional 方法：                             │
// │    进入方法前 → TransactionManager.getTransaction() → 开启/加入事务            │
// │    方法正常返回 → commit()                                                      │
// │    方法抛出异常 → rollback()（默认仅 RuntimeException/Error 触发回滚）           │
// │  底层本质：对 JDBC Connection.setAutoCommit(false) 的封装，                     │
// │  事务绑定在当前线程的 ThreadLocal<Connection> 上。                              │
// │                                                                                  │
// │  ★ 注意：@Transactional 只对 Spring 管理的代理对象生效，同类内部方法直接调用    │
// │    （this.xxx()）会绕过代理，导致事务注解失效！                                 │
// └───────────────────────────────────────────────────────────────────────────────┘
//
// ┌─ 四、Spring 事务传播行为（Propagation）───────────────────────────────────────┐
// │  REQUIRED（默认）  ：有外层事务则加入，没有则新建。本类所有方法均使用此默认值。   │
// │  REQUIRES_NEW      ：总是新建事务，挂起当前事务（适合独立记录日志等场景）。       │
// │  SUPPORTS          ：有事务则加入，没有也能运行（不保证原子性）。                 │
// │  MANDATORY         ：必须在现有事务内运行，否则抛异常。                          │
// │  NOT_SUPPORTED     ：挂起事务，以非事务方式运行。                                │
// │  NEVER             ：不允许在事务内运行，否则抛异常。                            │
// │  NESTED            ：嵌套事务（利用 Savepoint），内层回滚不影响外层。             │
// └───────────────────────────────────────────────────────────────────────────────┘

/**
 * 面试持久化服务
 * 面试会话和答案的持久化
 */
@Slf4j
// @Slf4j：Lombok 注解，自动生成 private static final Logger log = LoggerFactory.getLogger(XXX.class)
// 省去手写日志声明的样板代码。

@Service
// @Service：Spring 的 @Component 派生注解，将此类注册为 Spring Bean（Service 层语义标记）。

@RequiredArgsConstructor
// @RequiredArgsConstructor：Lombok 注解，自动为所有 final 字段生成构造函数。
public class InterviewPersistenceService {

    // ── 依赖注入（构造器注入，由 @RequiredArgsConstructor 生成） ──────────────────
    private final InterviewSessionRepository sessionRepository;
    // JPA Repository：封装面试会话表（interview_session）的 CRUD 操作。
    // Spring Data JPA 在运行时生成代理实现，底层通过 EntityManager 操作数据库。

    private final InterviewAnswerRepository answerRepository;
    // JPA Repository：封装面试答案表（interview_answer）的 CRUD 操作。

    private final ResumeRepository resumeRepository;
    // JPA Repository：封装简历表（resume）的 CRUD 操作，此处仅用于关联查询。

    private final ObjectMapper objectMapper;
    // Jackson 的核心序列化/反序列化工具类。
    // Spring Boot 自动配置了一个单例 ObjectMapper Bean，线程安全，可直接注入复用。
    // ★ ObjectMapper 本身是线程安全的，多线程并发调用 writeValueAsString/readValue 无问题。

    @Lazy
    @Autowired
    private InterviewPersistenceService self;
    // ★ 自注入：用于在同类内部调用 @Transactional 方法时走 Spring AOP 代理，
    //   这里是 Spring 注入进来的 代理对象（Proxy）

    // ════════════════════════════════════════════════════════════════════════════
    // 方法一：saveSession —— 保存新的面试会话
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 保存新的面试会话（支持可选简历）
     */
    @Transactional(rollbackFor = Exception.class)
    // @Transactional：Spring 声明式事务注解，AOP 代理会在方法执行前后自动管理事务。
    //
    // rollbackFor = Exception.class：
    //   Spring 默认只对 RuntimeException（非受检异常）和 Error 触发回滚。
    //   显式指定 Exception.class 后，受检异常（如 JacksonException extends Exception）
    //   也会触发回滚，防止序列化失败时产生部分提交的脏数据。
    //
    // 传播行为（未指定，使用默认 REQUIRED）：
    //   若调用方已开启事务，则加入该事务；若无，则新建事务。
    //   本方法被单独调用时：新建事务 → 执行 → commit/rollback。
    //
    // 隔离级别（未指定，使用 Isolation.DEFAULT）：
    //   沿用底层数据库默认值：MySQL InnoDB = REPEATABLE_READ，PostgreSQL = READ_COMMITTED。
    //   此处为纯插入操作（INSERT），不涉及先读后写，隔离级别对本方法影响较小。
    public void saveSession(String sessionId, Long resumeId,
                            int totalQuestions,
                            List<InterviewQuestionDTO> questions,
                            String llmProvider,
                            String skillId,
                            String difficulty) {
        try {
            InterviewSessionEntity session = new InterviewSessionEntity();
            // 创建 JPA 实体对象（此时为 Transient 状态，尚未与数据库关联）。
            // JPA 实体有三种状态：
            //   Transient（瞬时）  → new 出来，未被 EntityManager 管理
            //   Persistent（持久） → 被 EntityManager 管理，变更会自动同步到 DB
            //   Detached（游离）   → 事务结束后，实体脱离 EntityManager 管理

            session.setSessionId(sessionId);

            session.setTotalQuestions(totalQuestions);

            session.setCurrentQuestionIndex(0);

            session.setStatus(InterviewSessionEntity.SessionStatus.CREATED);

            session.setQuestionsJson(objectMapper.writeValueAsString(questions));
            // 将题目列表序列化为 JSON 字符串存入数据库（反范式设计，避免多表关联）。
            // objectMapper.writeValueAsString() 若序列化失败抛出 JacksonException（受检异常），
            // 由外层 catch 捕获并转为 BusinessException，触发事务回滚（rollbackFor 覆盖了受检异常）。

            session.setLlmProvider(llmProvider != null ? llmProvider : "default");

            session.setSkillId(skillId != null ? skillId : InterviewDefaults.SKILL_ID);

            session.setDifficulty(difficulty != null ? difficulty : InterviewDefaults.DIFFICULTY);

            // 简历可选：有 resumeId 则关联简历
            if (resumeId != null) {
                Optional<ResumeEntity> resumeOpt = resumeRepository.findById(resumeId);
                // SELECT * FROM resume WHERE id = resumeId

                resumeOpt.ifPresent(session::setResume);
            }

            sessionRepository.save(session);
            // JPA save()：
            //   - 实体 id 为 null → 调用 EntityManager.persist()（INSERT）
            //   - 实体 id 不为 null → 调用 EntityManager.merge()（UPDATE）
            // 本方法传入的是新建实体（id 为 null），执行 INSERT。

            log.info("面试会话已保存: sessionId={}, skillId={}, resumeId={}", sessionId, skillId, resumeId);

            // 返回已持久化的实体（包含数据库生成的 id、createTime 等字段）。
            // 事务在此方法返回后由 AOP 代理提交（commit），数据此时才真正写入数据库。

        } catch (JacksonException e) {
            // 题目列表 JSON 序列化失败（受检异常），触发事务回滚，返回专用错误码
            log.error("序列化问题列表失败: sessionId={}, error={}", sessionId, e.getMessage(), e);
            throw new BusinessException(ErrorCode.INTERVIEW_SERIALIZATION_ERROR, "序列化问题列表失败");
        } catch (DataIntegrityViolationException e) {
            // 数据库约束违例（如 sessionId 唯一键冲突），触发事务回滚
            log.error("面试会话持久化约束违例: sessionId={}, error={}", sessionId, e.getMessage(), e);
            throw new BusinessException(ErrorCode.INTERVIEW_SESSION_CREATE_FAILED,
                    "面试会话创建失败，请稍后重试");
        } catch (DataAccessException e) {
            // 其他数据库访问异常（连接失败、超时等），触发事务回滚
            log.error("面试会话持久化失败: sessionId={}, error={}", sessionId, e.getMessage(), e);
            throw new BusinessException(ErrorCode.INTERVIEW_PERSISTENCE_FAILED,
                    "面试数据持久化失败，请稍后重试");
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法二：updateSessionStatus —— 更新会话状态（SELECT FOR UPDATE 防 Lost Update）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 更新会话状态。使用悲观行锁（FOR UPDATE）串行化对同一行的并发修改。
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateSessionStatus(String sessionId, InterviewSessionEntity.SessionStatus status) {
        sessionRepository.findBySessionIdForUpdate(sessionId).ifPresent(session -> {
            session.setStatus(status);
            if (status == InterviewSessionEntity.SessionStatus.COMPLETED
                    || status == InterviewSessionEntity.SessionStatus.EVALUATED) {
                session.setCompletedAt(LocalDateTime.now());
            }
            sessionRepository.save(session);
        });
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法三：updateEvaluateStatus —— 更新评估状态（SELECT FOR UPDATE 防 Lost Update）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 更新评估状态。使用悲观行锁防止异步评估任务的并发回调互相覆盖。
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateEvaluateStatus(String sessionId, AsyncTaskStatus status, String error) {
        sessionRepository.findBySessionIdForUpdate(sessionId).ifPresent(session -> {
            session.setEvaluateStatus(status);
            // 租约维护：进入 PROCESSING 记录开始时间，回到 PENDING 清空。
            // 保证不变式" evaluateStatus = PROCESSING ⇒ evaluateStartedAt 非空且为最近一次开始时间 "，
            // 供补偿任务识别"崩溃卡死"（租约过期仍停在 PROCESSING）的会话。
            if (status == AsyncTaskStatus.PROCESSING) {
                session.setEvaluateStartedAt(LocalDateTime.now());
            } else if (status == AsyncTaskStatus.PENDING) {
                session.setEvaluateStartedAt(null);
            }
            if (error != null) {
                session.setEvaluateError(error.length() > 500 ? error.substring(0, 500) : error);
            } else {
                session.setEvaluateError(null);
            }
            sessionRepository.save(session);
            log.debug("评估状态已更新: sessionId={}, status={}", sessionId, status);
        });
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法四：updateCurrentQuestionIndex —— 更新当前题目索引（SELECT FOR UPDATE 防索引倒退）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 更新当前问题索引。使用悲观行锁防止并发修改导致的索引倒退。
     */
    @Transactional(rollbackFor = Exception.class)
    public void updateCurrentQuestionIndex(String sessionId, int index) {
        sessionRepository.findBySessionIdForUpdate(sessionId).ifPresent(session -> {
            session.setCurrentQuestionIndex(index);
            if (session.getStatus() == InterviewSessionEntity.SessionStatus.CREATED) {
                session.setStatus(InterviewSessionEntity.SessionStatus.IN_PROGRESS);
            }
            sessionRepository.save(session);
        });
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法五：saveAnswer —— 保存面试答案
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 保存面试答案。
     * 通过 {@link #upsertAnswer} 利用 DB 唯一约束保证并发安全，避免 Check-Then-Act 竞态。
     */
    @Transactional(rollbackFor = Exception.class)
    public InterviewAnswerEntity saveAnswer(String sessionId, int questionIndex,
                                            String question, String category,
                                            String userAnswer, int score, String feedback) {
        InterviewSessionEntity session = sessionRepository.findBySessionIdForUpdate(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));

        InterviewAnswerEntity saved = upsertAnswer(sessionId, session, questionIndex,
                question, category, userAnswer, score, feedback);

        log.info("面试答案已保存: sessionId={}, questionIndex={}, score={}",
                sessionId, questionIndex, score);
        return saved;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // submitAnswer —— 提交答案（saveAnswer + updateCurrentIndex + updateStatus 原子组合）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 提交答案：在同一事务内原子执行答案保存、索引进阶、状态更新。
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    // 传播行为：REQUIRES_NEW —— 每次调用都开启一个全新的独立事务。
    //   调用方（InterviewSessionService）自身无事务，此方法每次执行都是独立事务单元。
    public void submitAnswer(String sessionId, int questionIndex,
                             String question, String category,
                             String userAnswer, int newIndex,
                             InterviewSessionEntity.SessionStatus newStatus) {
        // ── 1. 加载会话（SELECT FOR UPDATE 防 Lost Update） ─────────────────
        InterviewSessionEntity session = sessionRepository.findBySessionIdForUpdate(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));

        // ── 2. 保存答案（并发安全 upsert） ──────────────────────────────────
        upsertAnswer(sessionId, session, questionIndex, question, category,
                userAnswer, 0, null);

        // ── 3. 更新当前题目索引 ────────────────────────────────────────────
        session.setCurrentQuestionIndex(newIndex);

        // ── 4. 更新会话状态 ────────────────────────────────────────────────
        session.setStatus(newStatus);
        if (newStatus == InterviewSessionEntity.SessionStatus.COMPLETED
                || newStatus == InterviewSessionEntity.SessionStatus.EVALUATED) {
            session.setCompletedAt(LocalDateTime.now());
        }
        sessionRepository.save(session);

        log.info("提交答案完成: sessionId={}, questionIndex={}, newIndex={}, newStatus={}",
                sessionId, questionIndex, newIndex, newStatus);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // saveAnswerDraft —— 暂存答案（saveAnswer + 可选 updateStatus 原子组合）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 暂存答案：在同一事务内原子执行答案保存与会话状态推进。
     * 将原先两个独立事务合并，消除"Redis 先写成功、MySQL 后写失败导致数据丢失"的问题。
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    // 传播行为：REQUIRES_NEW —— 每次调用都开启一个全新的独立事务。
    //   原子性：saveAnswer + updateSessionStatus 在同一次 commit 中完成，
    //   彻底消除原先两个独立事务的部分提交风险。
    public void saveAnswerDraft(String sessionId, int questionIndex,
                                String question, String category, String userAnswer) {
        // ── 1. 加载会话（SELECT FOR UPDATE 防 Lost Update） ─────────────────
        InterviewSessionEntity session = sessionRepository.findBySessionIdForUpdate(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));

        // ── 2. 保存/更新答案（并发安全 upsert） ─────────────────────────
        upsertAnswer(sessionId, session, questionIndex, question, category,
                userAnswer, 0, null);

        // ── 3. 首次暂存时将状态从 CREATED 推进为 IN_PROGRESS ──────────
        if (session.getStatus() == InterviewSessionEntity.SessionStatus.CREATED) {
            session.setStatus(InterviewSessionEntity.SessionStatus.IN_PROGRESS);
            sessionRepository.save(session);
        }

        log.info("暂存答案完成: sessionId={}, questionIndex={}", sessionId, questionIndex);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // completeInterview —— 交卷（updateStatus + updateEvaluateStatus 原子组合）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 交卷：在同一事务内原子执行状态推进（COMPLETED）和评估标记（PENDING）。
     * 消除原先两个独立事务的部分提交风险。
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    // 传播行为：REQUIRES_NEW —— 每次调用都开启一个全新的独立事务。
    //   原子性：updateSessionStatus + updateEvaluateStatus 在同一次 commit 中完成。
    public void completeInterview(String sessionId) {
        InterviewSessionEntity session = sessionRepository.findBySessionIdForUpdate(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));

        session.setStatus(InterviewSessionEntity.SessionStatus.COMPLETED);
        session.setCompletedAt(LocalDateTime.now());
        session.setEvaluateStatus(AsyncTaskStatus.PENDING);
        session.setEvaluateError(null);
        session.setEvaluateStartedAt(null);
        sessionRepository.save(session);

        log.info("交卷完成: sessionId={}", sessionId);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // tryClaimForEvaluation —— CAS 抢占评估权，解决 generateReport 的 TOCTOU
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 尝试抢占评估权：用 SELECT ... FOR UPDATE 串行化对同一行的访问，
     * 只有成功将 evaluateStatus 推进到 PROCESSING 的调用方才允许继续执行 LLM 评估。
     *
     * @return true=抢占成功，调用方可继续 LLM；false=已被其他线程/Consumer 评估完成或进行中
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public boolean tryClaimForEvaluation(String sessionId) {

        InterviewSessionEntity session = sessionRepository.findBySessionIdForUpdate(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));
        // Fixme SELECT ... FOR UPDATE：串行化，同一时刻只有一个事务能拿到行锁 （锁释放的时机为，事务结束的时候）

        if (session.getStatus() == InterviewSessionEntity.SessionStatus.EVALUATED) {
            return false; // 已评估完毕
        }
        if (session.getEvaluateStatus() == AsyncTaskStatus.COMPLETED
                || session.getEvaluateStatus() == AsyncTaskStatus.PROCESSING) {
            return false; // 已评估完成或正在评估中（异步 Consumer）
        }

        session.setEvaluateStatus(AsyncTaskStatus.PROCESSING);
        session.setEvaluateStartedAt(LocalDateTime.now()); // 租约起点：供补偿任务识别崩溃卡死
        sessionRepository.save(session);
        log.info("抢占评估权成功: sessionId={}", sessionId);
        return true;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // resetStaleProcessing —— 租约过期重置（崩溃卡死自愈）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 重置"租约过期"的 PROCESSING 会话回 PENDING，供补偿任务识别崩溃卡死的执行者。
     * <p>
     * 双检锁模式：先由补偿任务按 {@code evaluateStartedAt < leaseStart} 无锁扫描，再用
     * SELECT ... FOR UPDATE 加锁复核——仅在确认仍是"过期 PROCESSING"时才重置，
     * 避免与其实还在执行（只是慢）的消费者竞争误伤。
     *
     * @param sessionId  会话 ID
     * @param leaseStart 租约起始阈值（早于该时间的 PROCESSING 视为过期）
     * @return true=已重置为 PENDING；false=状态已变化或租约未过期，无需处理
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public boolean resetStaleProcessing(String sessionId, LocalDateTime leaseStart) {
        InterviewSessionEntity session = sessionRepository.findBySessionIdForUpdate(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));

        if (session.getEvaluateStatus() != AsyncTaskStatus.PROCESSING) {
            return false; // 已被推进（COMPLETED 等），无需重置
        }
        if (session.getEvaluateStartedAt() != null && session.getEvaluateStartedAt().isAfter(leaseStart)) {
            return false; // 租约未过期：消费者仍在执行或刚刷新 lease，不误伤
        }
        // startedAt 为 null（历史残留）或早于阈值 → 视为过期

        // 过期：重置回 PENDING，清空租约与错误，重新走 投递 + claim 抢占 恢复执行
        session.setEvaluateStatus(AsyncTaskStatus.PENDING);
        session.setEvaluateStartedAt(null);
        session.setEvaluateError(null);
        sessionRepository.save(session);
        log.warn("已重置租约过期的 PROCESSING 为 PENDING: sessionId={}", sessionId);
        return true;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法六：saveReport —— 保存面试报告（复合并发问题）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 保存面试报告(将 InterviewReportDTO 中的内容，写到 InterviewSessionEntity 以及 InterviewAnswerEntity 中去)
     */
    @Transactional(rollbackFor = Exception.class)
    // 传播行为：REQUIRED（默认）。
    // 隔离级别：DEFAULT（跟随数据库默认值）。
    public void saveReport(String sessionId, InterviewReportDTO report) {
        try {
            // Fixme 悲观锁的实现
            Optional<InterviewSessionEntity> sessionOpt = sessionRepository.findBySessionIdForUpdate(sessionId);
            // SELECT ... FOR UPDATE：串行化对同一行的并发修改，防止 saveReport 的
            // session 字段更新与其他并发写操作（如 completeInterview）之间丢失更新。

            if (sessionOpt.isEmpty()) {
                log.warn("会话不存在: {}", sessionId);
                // 静默返回，事务正常结束（commit 空事务）。
                return;
            }

            InterviewSessionEntity session = sessionOpt.get();

            // 设置总评分。
            session.setOverallScore(report.overallScore());

            // 设置总体反馈文本。
            session.setOverallFeedback(report.overallFeedback());

            // 将优势列表序列化为 JSON 存储。
            session.setStrengthsJson(objectMapper.writeValueAsString(report.strengths()));

            // 将改进建议序列化为 JSON 存储
            session.setImprovementsJson(objectMapper.writeValueAsString(report.improvements()));

            // 将参考答案序列化为 JSON 存储。
            session.setReferenceAnswersJson(objectMapper.writeValueAsString(report.referenceAnswers()));

            // 状态推进到 EVALUATED（已评估完成）。
            session.setStatus(InterviewSessionEntity.SessionStatus.EVALUATED);

            // 记录完成时间（应用服务器时间）。
            session.setCompletedAt(LocalDateTime.now());

            // 持久化 session 的所有变更。
            sessionRepository.save(session);

            // 查询已存在的答案，建立索引
            List<InterviewAnswerEntity> existingAnswers = answerRepository.findBySession_SessionIdOrderByQuestionIndex(sessionId);

            Map<Integer, InterviewAnswerEntity> answerMap = existingAnswers.stream()
                    .collect(java.util.stream.Collectors.toMap(
                            InterviewAnswerEntity::getQuestionIndex,
                            a -> a,
                            (a1, a2) -> a1
                            // 合并函数：若存在重复 questionIndex，保留第一个。
                            // 正常情况下不应有重复（业务保证），此为防御性代码。
                    ));
            // 将答案列表转为 Map<questionIndex, entity>，O(1) 查找。

            // 建立参考答案索引
            Map<Integer, InterviewReportDTO.ReferenceAnswer> refAnswerMap = report.referenceAnswers().stream()
                    .collect(java.util.stream.Collectors.toMap(
                            InterviewReportDTO.ReferenceAnswer::questionIndex,
                            r -> r,
                            (r1, r2) -> r1
                            // 同上，防御性合并。
                    ));
            // 将参考答案按 questionIndex 建立索引，便于后续 O(1) 查找。

            List<InterviewAnswerEntity> answersToSave = new ArrayList<>();
            // 收集所有需要持久化的答案实体，最后批量 saveAll 一次性提交，
            // 避免在循环中多次调用 save（每次 save 可能触发一次 SQL，性能较差）。

            // 遍历所有评估结果，更新或创建答案记录
            for (InterviewReportDTO.QuestionEvaluation eval : report.questionDetails()) {
                // 遍历本次评估报告中的每道题目评估详情。

                InterviewAnswerEntity answer = answerMap.get(eval.questionIndex());
                // 尝试从已有答案 Map 中找到对应记录。

                if (answer == null) {
                    // 未回答的题目，创建新记录
                    answer = new InterviewAnswerEntity();
                    answer.setSession(session);
                    answer.setQuestionIndex(eval.questionIndex());
                    answer.setQuestion(eval.question());
                    answer.setCategory(eval.category());
                    answer.setUserAnswer(null);  // 未回答
                    log.debug("为未回答的题目 {} 创建答案记录", eval.questionIndex());
                }

                // 更新评分和反馈
                answer.setScore(eval.score());
                answer.setFeedback(eval.feedback());

                // 设置参考答案和关键点
                InterviewReportDTO.ReferenceAnswer refAns = refAnswerMap.get(eval.questionIndex());
                if (refAns != null) {
                    answer.setReferenceAnswer(refAns.referenceAnswer());
                    if (refAns.keyPoints() != null && !refAns.keyPoints().isEmpty()) {
                        answer.setKeyPointsJson(objectMapper.writeValueAsString(refAns.keyPoints()));
                    }
                }

                answersToSave.add(answer);
            }

            answerRepository.saveAll(answersToSave);
            // 批量持久化所有答案（一次 SQL 批处理，比循环 save 性能更好）。
            // Spring Data JPA 的 saveAll 底层调用 EntityManager.persist/merge，
            // 实际是否真正批量取决于 JPA 实现（Hibernate）和 spring.jpa.properties.hibernate.jdbc.batch_size 配置。

            log.info("面试报告已保存: sessionId={}, score={}, 答案数={}",
                    sessionId, report.overallScore(), answersToSave.size());

        } catch (JacksonException e) {
            // 序列化失败必须中止事务，否则 session.status 已改为 EVALUATED 但 JSON 字段仍是旧值，造成不可逆的数据不一致（半截报告）。
            log.error("序列化报告失败，事务回滚: sessionId={}, error={}", sessionId, e.getMessage(), e);
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "序列化面试报告失败");
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法七：findBySessionId —— 根据会话ID获取会话（只读）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 根据会话ID获取会话
     */
    @Transactional(readOnly = true)
    public Optional<InterviewSessionEntity> findBySessionId(String sessionId) {
        return sessionRepository.findBySessionId(sessionId);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法八：findByResumeId —— 获取简历的所有面试记录（只读）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 获取简历的所有面试记录
     */
    @Transactional(readOnly = true)
    public List<InterviewSessionEntity> findByResumeId(Long resumeId) {
        return sessionRepository.findByResumeIdOrderByCreatedAtDesc(resumeId);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法九：findAll —— 获取所有面试记录（只读）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 获取所有面试记录（按创建时间倒序）
     */
    @Transactional(readOnly = true)
    public List<InterviewSessionEntity> findAll() {
        return sessionRepository.findAllByOrderByCreatedAtDesc();
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十：deleteSessionsByResumeId —— 批量删除简历的所有面试会话（JPQL 原子删除）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 批量删除简历的所有面试会话及答案。
     * 使用 JPQL 直接 DELETE，消除先查后删的 TOCTOU 窗口和 N+1 性能问题。
     */
    @Transactional(rollbackFor = Exception.class)
    public void deleteSessionsByResumeId(Long resumeId) {
        int answersDeleted = answerRepository.deleteByResumeId(resumeId);
        int sessionsDeleted = sessionRepository.deleteByResumeId(resumeId);
        log.info("已批量删除简历 {} 的面试数据: 答案 {} 条, 会话 {} 个",
                resumeId, answersDeleted, sessionsDeleted);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十一：deleteSessionBySessionId —— 删除单个面试会话（SELECT FOR UPDATE 防 TOCTOU）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 删除单个面试会话。
     * CascadeType.ALL + orphanRemoval = true 自动级联删除关联答案。
     * 使用 SELECT FOR UPDATE 防止两个线程同时删除同一会话时出现竞态。
     */
    @Transactional(rollbackFor = Exception.class)
    public void deleteSessionBySessionId(String sessionId) {
        InterviewSessionEntity session = sessionRepository.findBySessionIdForUpdate(sessionId)
                .orElseThrow(() -> new BusinessException(ErrorCode.INTERVIEW_SESSION_NOT_FOUND));
        sessionRepository.delete(session);
        log.info("已删除面试会话: sessionId={}", sessionId);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十二：findUnfinishedSession —— 查找未完成的面试会话（只读）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 查找未完成的面试会话（CREATED或IN_PROGRESS状态）
     */
    @Transactional(readOnly = true)
    public Optional<InterviewSessionEntity> findUnfinishedSession(Long resumeId) {
        List<InterviewSessionEntity.SessionStatus> unfinishedStatuses = List.of(
                InterviewSessionEntity.SessionStatus.CREATED,
                InterviewSessionEntity.SessionStatus.IN_PROGRESS
        );
        // List.of() 返回不可变列表（Java 9+），防止调用方意外修改状态列表。

        return sessionRepository.findFirstByResumeIdAndStatusInOrderByCreatedAtDesc(resumeId, unfinishedStatuses);
        // SELECT * FROM interview_session
        // WHERE resume_id = ? AND status IN ('CREATED', 'IN_PROGRESS')
        // ORDER BY created_at DESC LIMIT 1
        // 取最新创建的一条未完成会话（若有多条，只返回最新的）。
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十三：findAnswersBySessionId —— 根据会话ID查找所有答案（只读）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 根据会话ID查找所有答案
     */
    @Transactional(readOnly = true)
    public List<InterviewAnswerEntity> findAnswersBySessionId(String sessionId) {
        return answerRepository.findBySession_SessionIdOrderByQuestionIndex(sessionId);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 常量：历史题目最大数量
    // ════════════════════════════════════════════════════════════════════════════

    private static final int MAX_HISTORICAL_QUESTIONS = 60;
    // static final：类级别常量，所有实例共享，JVM 加载类时初始化，线程安全。
    // 用于限制返回的历史题目数量，防止数据量过大影响 LLM 上下文长度或接口性能。

    // ════════════════════════════════════════════════════════════════════════════
    // 方法十四：getHistoricalQuestions —— 获取历史提问列表（只读 + 内存计算）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * 获取历史提问列表（结构化，按分类压缩用）。
     * 有 resumeId 时精确匹配 resumeId + skillId；无 resumeId 时按 skillId 查全部（通用模式兜底）。
     */
    @Transactional(readOnly = true)
    public List<HistoricalQuestion> getHistoricalQuestions(String skillId, Long resumeId) {
        List<InterviewSessionEntity> sessions;
        if (resumeId != null) {
            sessions = sessionRepository.findTop10ByResumeIdAndSkillIdOrderByCreatedAtDesc(resumeId, skillId);
            // SELECT * FROM interview_session
            // WHERE resume_id = ? AND skill_id = ?
            // ORDER BY created_at DESC LIMIT 10
            // 有简历时，精确匹配该简历在该技能领域的最近 10 次面试。
        } else {
            sessions = sessionRepository.findTop10BySkillIdOrderByCreatedAtDesc(skillId);
            // SELECT * FROM interview_session
            // WHERE skill_id = ?
            // ORDER BY created_at DESC LIMIT 10
            // 无简历时（通用模式），查该技能领域全局最近 10 次面试。
            // ★ 注意：无 resumeId 时查询跨用户数据，若系统有多用户隔离需求，此处存在数据泄露风险。
        }

        log.info("加载历史题目: skillId={}, resumeId={}, 查到 {} 个历史会话", skillId, resumeId, sessions.size());

        LinkedHashSet<String> seen = new LinkedHashSet<>();
        // LinkedHashSet：维护插入顺序的 HashSet，用于去重同时保持题目出现顺序。
        // 此为方法局部变量，每次调用独立创建，无多线程共享，线程安全。

        List<HistoricalQuestion> result = sessions.stream()
                // 流式处理 session 列表，惰性求值，链式操作。

                .map(InterviewSessionEntity::getQuestionsJson)
                // 从每个 session 实体中提取 questionsJson 字符串（方法引用等价于 s -> s.getQuestionsJson()）。

                .filter(json -> json != null && !json.isEmpty())
                // 过滤掉空 JSON（防止历史数据中存在无题目的异常会话）。

                .flatMap(json -> {
                    // flatMap：将每个 session 的题目列表"展平"成单一流，
                    // 实现多个 session → 多个 question 的一对多扁平化。
                    try {
                        List<InterviewQuestionDTO> questions = objectMapper.readValue(json,
                                new TypeReference<List<InterviewQuestionDTO>>() {
                                });
                        // 反序列化 JSON 字符串 → List<InterviewQuestionDTO>。
                        // TypeReference 解决 Java 泛型擦除问题，使 Jackson 能正确推断泛型类型。
                        // objectMapper 是线程安全的，并发调用无问题。

                        return questions.stream()
                                .filter(q -> !q.isFollowUp())
                                // 过滤掉追问题（follow-up question），只保留主问题，
                                // 避免历史去重时将追问也纳入"已考过"的范畴。
                                .map(q -> new HistoricalQuestion(q.question(), q.type(), q.topicSummary()));
                        // 将 InterviewQuestionDTO 映射为轻量的 HistoricalQuestion 记录类。
                    } catch (Exception e) {
                        log.error("解析历史问题JSON失败", e);
                        return java.util.stream.Stream.<HistoricalQuestion>empty();
                        // 解析失败时返回空流（跳过该 session），避免单条数据错误中断整个流处理。
                        // 这是防御性编程，但会静默忽略数据异常，建议配合监控告警。
                    }
                })

                .filter(hq -> seen.add(hq.question()))
                // seen.add() 若元素已存在返回 false，利用 Set 的 add 返回值实现流式去重。
                // 效果：相同题目只保留第一次出现（按 session 时间倒序，即最近出现的题目）。
                // ★ LinkedHashSet.add() 是非线程安全的，但此处 seen 是局部变量，单线程操作，安全。

                .limit(MAX_HISTORICAL_QUESTIONS)
                // 限制最多返回 60 道历史题目，超出部分截断。
                // limit 是短路操作（short-circuit），找到足够数量后停止处理后续元素，性能友好。

                .toList();
        // Java 16+ 的 Stream.toList()，返回不可变 List（等价于 Collectors.toUnmodifiableList()）。
        // 比 collect(Collectors.toList()) 更简洁，且返回的 List 不可修改，防止调用方意外篡改。

        log.info("历史题目加载完成: 去重后 {} 道主问题，按分类: {}", result.size(),
                result.stream().collect(java.util.stream.Collectors.groupingBy(
                        hq -> hq.type() != null ? hq.type() : "GENERAL",
                        // 若题目类型为 null，归入 "GENERAL" 分类（防御性处理）。
                        java.util.stream.Collectors.counting()
                        // 统计每个分类的题目数量。
                )));
        // 日志中打印按分类分组的题目数量，便于排查历史题目分布是否均匀。

        return result;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 工具方法：答案 Upsert（并发安全）
    // ════════════════════════════════════════════════════════════════════════════

    /**
     * Upsert 答案：利用 DB 唯一约束 (session_id, question_index) 做并发安全兜底。
     * <p>
     * 先查已有记录 → 有则更新；无则通过独立事务尝试插入，
     * 若并发插入冲突则退化为更新（利用 DB 唯一约束的原子性保证最终一致性）后写的会覆盖之前写的。
     */
    private InterviewAnswerEntity upsertAnswer(String sessionId, InterviewSessionEntity session,
                                               int questionIndex, String question,
                                               String category, String userAnswer,
                                               Integer score, String feedback) {
        // 快速路径：已有记录直接更新（覆盖绝大部分调用场景）
        Optional<InterviewAnswerEntity> existing = answerRepository
                .findBySession_SessionIdAndQuestionIndex(sessionId, questionIndex);
        if (existing.isPresent()) {
            InterviewAnswerEntity answer = existing.get();
            answer.setQuestion(question);
            answer.setCategory(category);
            answer.setUserAnswer(userAnswer);
            answer.setScore(score);
            answer.setFeedback(feedback);
            return answerRepository.save(answer);
        }
        // 慢路径：尝试插入，若并发冲突则退化为更新
        try {
            // Fixme 代理对象调用事务方法，事务会照常执行
            return self.tryInsertAnswer(session, questionIndex, question, category,
                    userAnswer, score, feedback);
        } catch (DataIntegrityViolationException e) {
            log.info("并发插入冲突，退化为更新: sessionId={}, questionIndex={}",
                    sessionId, questionIndex);
            InterviewAnswerEntity answer = answerRepository
                    .findBySession_SessionIdAndQuestionIndex(sessionId, questionIndex)
                    .orElseThrow(() -> new BusinessException(ErrorCode.INTERNAL_ERROR,
                            "并发答案插入失败: sessionId=" + sessionId));
            answer.setQuestion(question);
            answer.setCategory(category);
            answer.setUserAnswer(userAnswer);
            answer.setScore(score);
            answer.setFeedback(feedback);
            return answerRepository.save(answer);
        }
    }

    /**
     * 在独立事务中插入答案。供 {@link #upsertAnswer} 调用。
     * <p>
     * <b>必须通过 self 代理调用（而非 this），否则 {@code @Transactional} 不会生效。</b>
     * 独立事务保证插入失败（唯一约束冲突）时不影响外层事务的持久化上下文。
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    InterviewAnswerEntity tryInsertAnswer(InterviewSessionEntity session, int questionIndex,
                                          String question, String category, String userAnswer,
                                          Integer score, String feedback) {
        InterviewAnswerEntity answer = new InterviewAnswerEntity();
        answer.setSession(session);
        answer.setQuestionIndex(questionIndex);
        answer.setQuestion(question);
        answer.setCategory(category);
        answer.setUserAnswer(userAnswer);
        answer.setScore(score);
        answer.setFeedback(feedback);
        answerRepository.saveAndFlush(answer);
        return answer;
    }
}

