package interview.guide.modules.interview.listener;

import interview.guide.common.async.AbstractStreamConsumer;
import interview.guide.common.ai.LlmProviderRegistry;
import interview.guide.common.config.AsyncConsumerProperties;
import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.infrastructure.redis.InterviewSessionCache;
import interview.guide.infrastructure.redis.RedisService;
import interview.guide.modules.interview.model.InterviewAnswerEntity;
import interview.guide.modules.interview.model.InterviewQuestionDTO;
import interview.guide.modules.interview.model.InterviewReportDTO;
import interview.guide.modules.interview.model.InterviewSessionEntity;
import interview.guide.modules.interview.repository.InterviewSessionRepository;
import interview.guide.modules.interview.service.AnswerEvaluationService;
import interview.guide.modules.interview.service.InterviewPersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component  // 注册为 Spring Bean，@PostConstruct 才会被触发，消费者线程才会启动
public class EvaluateStreamConsumer
        // 泛型填入 EvaluatePayload，告诉父类：本消费者处理的消息载体类型
        extends AbstractStreamConsumer<EvaluateStreamConsumer.EvaluatePayload> {

    // ─────────────────────────────────────────────
    // 依赖注入
    // ─────────────────────────────────────────────

    private final InterviewSessionRepository sessionRepository;   // 操作 interview_session 表
    private final AnswerEvaluationService evaluationService;      // 操作 答案 相关服务
    private final InterviewPersistenceService persistenceService; // 查答案 + 存报告
    private final ObjectMapper objectMapper;                      // JSON 序列化/反序列化
    private final LlmProviderRegistry llmProviderRegistry;        // 根据 provider 名拿 ChatClient
    private final InterviewSessionCache sessionCache;             // 会话缓存（写失效用）
    private final AsyncConsumerProperties consumerProperties;     // 组内消费者并行数配置

    // ─────────────────────────────────────────────
    // 构造函数
    // ─────────────────────────────────────────────

    public EvaluateStreamConsumer(
            RedisService redisService,           // 传给父类，父类用它读写 Stream
            InterviewSessionRepository sessionRepository,
            AnswerEvaluationService evaluationService,
            InterviewPersistenceService persistenceService,
            ObjectMapper objectMapper,
            LlmProviderRegistry llmProviderRegistry,
            InterviewSessionCache sessionCache,
            AsyncConsumerProperties consumerProperties
    ) {
        super(redisService); // 父类持有 redisService，负责 XREADGROUP / XACK 等操作
        this.sessionRepository = sessionRepository;
        this.evaluationService = evaluationService;
        this.persistenceService = persistenceService;
        this.objectMapper = objectMapper;
        this.llmProviderRegistry = llmProviderRegistry;
        this.sessionCache = sessionCache;
        this.consumerProperties = consumerProperties;
    }

    // ─────────────────────────────────────────────
    // 消息载体定义
    // ─────────────────────────────────────────────

    /**
     * Stream 消息的业务载体（Java Record，不可变对象 类似于 内部类的使用方法）
     * <p>
     * 问题：为什么不直接用 sessionId 字符串，而要包成一个 Record？
     * 答：
     * 1. 类型安全：父类泛型 T 需要一个具体类型，String 太宽泛
     * 2. 可扩展：将来要传 userId、priority 等字段，直接加字段即可
     * 3. 语义清晰：EvaluatePayload 比 String 更能表达"这是评估任务的载体"
     * <p>
     * 数据来源完整链路：
     * <p>
     * 生产者（某个 Service）
     * └─ XADD interview:evaluate:stream * sessionId abc-123 retryCount 0
     * ↓
     * Redis Stream 存储：{ messageId: "1715...-0", fields: {sessionId: "abc-123", retryCount: "0"} }
     * ↓
     * 父类 XREADGROUP 拉取 → data = Map{"sessionId":"abc-123", "retryCount":"0"}
     * ↓
     * parsePayload() 解析 → EvaluatePayload(sessionId = "abc-123")
     * ↓
     * processBusiness(payload) 使用 payload.sessionId() 查库执行业务
     */
    public record EvaluatePayload(String sessionId) {
    }

    // ─────────────────────────────────────────────
    // 基本配置（告诉父类操作哪个 Stream / 哪个 Group）
    // ─────────────────────────────────────────────

    @Override
    protected String taskDisplayName() {
        return "评估"; // 仅用于日志输出，如："评估 task completed: sessionId=abc-123"
    }

    @Override
    protected String streamKey() {
        // 父类用这个 key 执行：
        //   XREADGROUP GROUP ... STREAMS <streamKey> >
        //   XACK <streamKey> <groupName> <messageId>
        //   XGROUP CREATE <streamKey> <groupName> $
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_STREAM_KEY;
        // 实际值类似："interview:evaluate:stream"
    }

    @Override
    protected String groupName() {
        // 消费者组名，对应 XREADGROUP 命令里的 GROUP 参数
        // 同一个组内的多个消费者实例会分摊消息（竞争消费）
        // 不同组之间各自独立消费全量消息
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_GROUP_NAME;
    }

    @Override
    protected String consumerPrefix() {
        // 父类会拼接成：consumerPrefix + UUID前8位
        // 如："evaluate-consumer-a3f2bc10"
        // 对应 XREADGROUP 命令里的 consumerName 参数
        // 多 Pod 部署时每个实例名字不同，Redis 可以区分哪条消息投给了谁
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_CONSUMER_PREFIX;
    }

    @Override
    protected String threadName() {
        return "evaluate-consumer"; // 线程名，在线程 dump / 监控工具里可识别
    }

    @Override
    protected int consumerCount() {
        // 组内并行消费者数由配置 app.async.consumers.evaluate 控制（默认 1）。
        // 并行后组内处理顺序不再保证，但每条消息仍只投给组内一个消费者；
        // 重复执行的兜底由 tryAcquire（tryClaimForEvaluation 的 DB 抢占）保证。
        return consumerProperties.getEvaluate();
    }

    // ─────────────────────────────────────────────
    // 消息解析：Map<String,String> → EvaluatePayload
    // ─────────────────────────────────────────────

    @Override
    protected EvaluatePayload parsePayload(StreamMessageId messageId, Map<String, String> data) {
        // data 就是 XADD 时写入的 fields，此处取出 sessionId
        //
        // 举例，data 的内容：
        //   {
        //     "sessionId"  : "abc-123-def-456",
        //     "retryCount" : "0"
        //   }
        //
        // 对应原生 Redis：XRANGE interview:evaluate:stream - +
        // 返回：1) 1715000000000-0
        //       2) 1) "sessionId"
        //          2) "abc-123-def-456"
        //          3) "retryCount"
        //          4) "0"
        String sessionId = data.get(AsyncTaskStreamConstants.FIELD_SESSION_ID);
        // FIELD_SESSION_ID 常量值就是字符串 "sessionId"

        if (sessionId == null) {
            // 消息字段缺失（数据损坏 / 生产者 bug），无法处理
            // 返回 null 后父类会直接 XACK 丢弃，避免卡死消费队列
            log.warn("消息格式错误，跳过: messageId={}", messageId);
            return null;
        }

        // 把 sessionId 包装成类型化的 Payload 对象返回给父类
        // retryCount 不放进来，父类 parseRetryCount() 会单独从 data 里解析
        // 普通类用 getXxx()，Record 用 xxx()，字段叫什么方法就叫什么。因此，这里使用sessionId() 方法，来获取record类型中的sessionId属性值
        return new EvaluatePayload(sessionId);
    }

    @Override
    protected String payloadIdentifier(EvaluatePayload payload) {
        // 用于日志标识，输出格式如："sessionId=abc-123-def-456"
        return "sessionId=" + payload.sessionId();
    }

    // ─────────────────────────────────────────────
    // 状态机：处理中
    // ─────────────────────────────────────────────

    @Override
    protected void markProcessing(EvaluatePayload payload) {
        // 在父类 processMessage 里，业务执行前先调用此方法
        // 数据库里 session 的 evaluateStatus 改为 PROCESSING
        // 作用：前端可以轮询这个状态，展示"评估中..."
        // 统一走 InterviewPersistenceService（悲观锁 + 事务），与生产者/补偿任务并发写时安全
        persistenceService.updateEvaluateStatus(payload.sessionId(), AsyncTaskStatus.PROCESSING, null);
    }

    @Override
    protected boolean tryAcquire(EvaluatePayload payload) {
        // 并发/幂等保护：CAS 抢占评估权（SELECT FOR UPDATE 原子地把 PENDING 推进为 PROCESSING）。
        try {
            return persistenceService.tryClaimForEvaluation(payload.sessionId());
        } catch (BusinessException e) {
            if (e.getCode().equals(ErrorCode.INTERVIEW_SESSION_NOT_FOUND.getCode())) {
                // 会话已被删除：返回 true，交给 processBusiness 的空检查静默跳过（按成功处理）
                log.warn("会话不存在，由业务检查跳过: sessionId={}", payload.sessionId());
                return true;
            }
            throw e;
        }
    }

    // ─────────────────────────────────────────────
    // 核心业务逻辑
    // ─────────────────────────────────────────────

    @Override
    protected void processBusiness(EvaluatePayload payload) {

        String sessionId = payload.sessionId();
        // 此时 sessionId = "abc-123-def-456"（从 Redis Stream 消息里解析来的）

        // ── Step 1：查询面试会话 ──────────────────────────────

        // SQL: SELECT s.*, r.* FROM interview_session s
        //      LEFT JOIN resume r ON s.resume_id = r.id
        //      WHERE s.session_id = ?
        // 同时 JOIN 了 resume，因为后续评估需要简历文本
        Optional<InterviewSessionEntity> sessionOpt =
                sessionRepository.findBySessionIdWithResume(sessionId);

        if (sessionOpt.isEmpty()) {
            // 会话已被用户删除，这条消息没有处理意义
            // 直接 return，父类会调用 markCompleted → XACK
            // 注意：这里不抛异常，是正常的"跳过"逻辑，而非失败
            log.warn("会话已被删除，跳过评估任务: sessionId={}", sessionId);
            return;
        }

        InterviewSessionEntity session = sessionOpt.get();

        // 幂等保护：会话已评估完成则跳过，避免重复 LLM 调用与报告覆盖。
        // saveReport 会将 session.status 设为 EVALUATED，因此此处以此作为"已完成"的权威信号。
        if (session.getStatus() == InterviewSessionEntity.SessionStatus.EVALUATED) {
            log.info("会话已评估，跳过重复任务: sessionId={}", sessionId);
            return;
        }

        // ── Step 2：解析题目列表 ──────────────────────────────

        // session.getQuestionsJson() 返回的是数据库里存的 JSON 字符串，例如：
        // [
        //   {"index":0, "question":"请介绍你的项目经历", "answer":null},
        //   {"index":1, "question":"你如何处理并发问题",  "answer":null}
        // ]
        //
        // objectMapper.readValue 把 JSON 字符串反序列化为 List<InterviewQuestionDTO>
        // TypeReference<>() 是 Jackson 的泛型类型擦除解决方案
        List<InterviewQuestionDTO> questions = objectMapper.readValue(
                session.getQuestionsJson(),
                new TypeReference<>() {
                }
        );

        // ── Step 3：查询用户答案，合并进题目列表 ──────────────

        // SQL: SELECT * FROM interview_answer WHERE session_id = ?
        List<InterviewAnswerEntity> answers =
                persistenceService.findAnswersBySessionId(sessionId);

        // 此时 questions 里每个 DTO 的 answer 字段还是 null
        // answers 里存着用户提交的答案，通过 questionIndex 对应

        for (InterviewAnswerEntity answer : answers) {
            int index = answer.getQuestionIndex(); // 第几道题的答案（0-based）

            if (index >= 0 && index < questions.size()) {
                InterviewQuestionDTO question = questions.get(index);

                // withAnswer() 是 Record 的方法，返回一个新的 DTO（answer 字段已填入）
                // 因为 InterviewQuestionDTO 是不可变 Record，不能直接 set，只能替换
                //
                // 合并后 questions[0] 变为：
                // {"index":0, "question":"请介绍你的项目经历", "answer":"我负责了XX模块..."}
                questions.set(index, question.withAnswer(answer.getUserAnswer()));
            }
        }

        // ── Step 4：获取对应的 LLM 客户端 ────────────────────

        // session 里存了创建时选择的模型，如 "openai" / "claude" / "deepseek"
        String provider = session.getLlmProvider();

        // 根据 provider 名从注册中心取对应的 ChatClient
        // 如果找不到就用默认的（防止配置错误导致空指针）
        ChatClient chatClient =
                llmProviderRegistry.getChatClientOrDefault(provider);

        // ── Step 5：调用 LLM 执行评估 ─────────────────────────

        // 简历文本，如果没有简历（用户未上传）则传空字符串
        String resumeText =
                session.getResume() != null ? session.getResume().getResumeText() : "";

        // evaluationService 内部会：
        //   1. 把 questions（含答案）+ resumeText 拼成 Prompt
        //   2. 调用 chatClient 发请求给 LLM
        //   3. 解析 LLM 返回的 JSON → InterviewReportDTO
        InterviewReportDTO report =
                evaluationService.evaluateInterview(chatClient, sessionId, resumeText, questions);

        // ── Step 6：保存报告到数据库 ──────────────────────────

        // SQL: INSERT INTO interview_report (...) VALUES (...)
        //  或: UPDATE interview_report SET ... WHERE session_id = ?
        persistenceService.saveReport(sessionId, report);

        // Cache-Aside 写失效：saveReport 已将 status 推进为 EVALUATED，删除会话缓存，
        // 避免前端读到"已完成但无报告"的旧缓存快照。下一次读取会从 MySQL 重新加载并回填。
        sessionCache.deleteSession(sessionId);

        // 至此 processBusiness 完成，控制权交回父类
        // 父类接着调用 markCompleted → XACK
    }

    // ─────────────────────────────────────────────
    // 状态机：完成
    // ─────────────────────────────────────────────

    @Override
    protected void markCompleted(EvaluatePayload payload) {
        // 更新 evaluateStatus = COMPLETED，前端轮询到后展示结果
        persistenceService.updateEvaluateStatus(payload.sessionId(), AsyncTaskStatus.COMPLETED, null);
    }

    // ─────────────────────────────────────────────
    // 状态机：最终失败（超过最大重试次数）
    // ─────────────────────────────────────────────

    @Override
    protected void markFailed(EvaluatePayload payload, String error) {
        // 更新 evaluateStatus = FAILED，同时记录错误信息供排查
        persistenceService.updateEvaluateStatus(payload.sessionId(), AsyncTaskStatus.FAILED, error);
    }

    // ─────────────────────────────────────────────
    // 重试机制：重新投递到 Stream
    // ─────────────────────────────────────────────

    @Override
    protected void retryMessage(EvaluatePayload payload, int retryCount, String error) {

        String sessionId = payload.sessionId();

        // 释放抢占：把状态重置回 PENDING，使下一条重试消息能再次通过 tryAcquire/tryClaimForEvaluation 抢占。
        // 同时记录本次失败原因，便于运维排查（下次抢占成功后会被 markProcessing 以 null 清空）。
        persistenceService.updateEvaluateStatus(sessionId, AsyncTaskStatus.PENDING, truncateError(error));

        try {
            // 指数退避：首次重试 1s，随后 2s、4s...（backoffMs = 2^(retryCount-1) * 1000）
            long backoffMs = (long) Math.pow(2, retryCount - 1) * 1000;

            // 构造新的消息 fields，和生产者投递时格式一样，额外携带退避时间与上次失败原因
            String lastError = truncateError(error);
            Map<String, String> message = Map.of(
                    AsyncTaskStreamConstants.FIELD_SESSION_ID, sessionId,
                    AsyncTaskStreamConstants.FIELD_RETRY_COUNT, String.valueOf(retryCount),
                    AsyncTaskStreamConstants.FIELD_DUE_AT, String.valueOf(System.currentTimeMillis() + backoffMs),
                    AsyncTaskStreamConstants.FIELD_LAST_ERROR, lastError != null ? lastError : "unknown"
            );

            // 【Redisson】重新写入 Stream
            // 对应原生 Redis 命令：
            //   XADD interview:evaluate:stream MAXLEN ~ 1000
            //        sessionId abc-123-def-456
            //        retryCount 2
            //        dueAt 1749000004000
            //
            // MAXLEN ~ 1000：近似裁剪，保证 Stream 长度不超过 1000
            //                ~ 表示"模糊"裁剪（性能更好，不保证精确）
            //
            // 重新入队后，消费者下次循环会拉到这条新消息；
            // 消费者读到 dueAt 未到时先等待，实现指数退避。
            redisService().streamAdd(
                    AsyncTaskStreamConstants.INTERVIEW_EVALUATE_STREAM_KEY,
                    message,
                    AsyncTaskStreamConstants.STREAM_MAX_LEN
            );

            log.warn("评估任务已重新入队（退避 {}ms）: sessionId={}, retryCount={}",
                    backoffMs, sessionId, retryCount);

        } catch (Exception e) {
            // 连重新入队都失败了（Redis 宕机等）
            // 没有更好的选择，只能标记为失败，人工排查
            log.error("重试入队失败: sessionId={}, error={}", sessionId, e.getMessage(), e);

            persistenceService.updateEvaluateStatus(
                    sessionId,
                    AsyncTaskStatus.FAILED,
                    truncateError("重试入队失败（消费者端）: " + e.getMessage())
                    // truncateError 继承自父类，截断到 500 字符，防止超长字符串写库报错
            );
        }
    }

    @Override
    protected String dlqKey() {
        return AsyncTaskStreamConstants.INTERVIEW_EVALUATE_DLQ_KEY;
    }

    @Override
    protected Map<String, String> buildDlqMessage(EvaluatePayload payload, int retryCount, String error) {
        String lastError = truncateError(error);
        return Map.of(
                AsyncTaskStreamConstants.FIELD_SESSION_ID, payload.sessionId(),
                AsyncTaskStreamConstants.FIELD_RETRY_COUNT, String.valueOf(retryCount),
                AsyncTaskStreamConstants.FIELD_LAST_ERROR, lastError != null ? lastError : "unknown",
                AsyncTaskStreamConstants.FIELD_FAILED_AT, String.valueOf(System.currentTimeMillis())
        );
    }

}