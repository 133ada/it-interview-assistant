package interview.guide.common.constant;

/**
 * 异步任务 Redis Stream 通用常量
 * 包含知识库向量化和简历分析两个异步任务的配置
 */
public final class AsyncTaskStreamConstants {

    private AsyncTaskStreamConstants() {
        // 私有构造函数，防止实例化
    }

    // ========== 通用消息字段 ==========

    /**
     * 重试次数字段
     */
    public static final String FIELD_RETRY_COUNT = "retryCount";

    /**
     * 文档内容字段
     */
    public static final String FIELD_CONTENT = "content";

    // ========== 通用消费者配置 ==========

    /**
     * 最大尝试次数（首次投递 + 最多 N-1 次重试）。
     * 例如 MAX_ATTEMPTS=3：共执行 3 次 processBusiness，之后进入最终失败（FAILED + DLQ）
     */
    public static final int MAX_ATTEMPTS = 3;

    /**
     * 重试最早可执行时间戳（毫秒）：指数退避用，未到时间的消息消费者先等待
     */
    public static final String FIELD_DUE_AT = "dueAt";

    /**
     * 最近一次失败原因
     */
    public static final String FIELD_LAST_ERROR = "lastError";

    /**
     * 失败时间戳（毫秒），写入死信队列时记录
     */
    public static final String FIELD_FAILED_AT = "failedAt";

    /**
     * 每次拉取的消息批次大小
     */
    public static final int BATCH_SIZE = 10;

    /**
     * 消费者轮询间隔（毫秒）
     */
    public static final long POLL_INTERVAL_MS = 1000;

    /**
     * Stream 最大长度（自动裁剪旧消息，防止无限增长）
     */
    public static final int STREAM_MAX_LEN = 1000;

    // ========== 知识库向量化 Stream 配置 ==========

    /**
     * 知识库向量化 Stream Key
     */
    public static final String KB_VECTORIZE_STREAM_KEY = "knowledgebase:vectorize:stream";

    /**
     * 知识库向量化 Consumer Group 名称
     */
    public static final String KB_VECTORIZE_GROUP_NAME = "vectorize-group";

    /**
     * 知识库向量化 Consumer 名称前缀
     */
    public static final String KB_VECTORIZE_CONSUMER_PREFIX = "vectorize-consumer-";

    /**
     * 知识库ID字段
     */
    public static final String FIELD_KB_ID = "kbId";

    // ========== 简历分析 Stream 配置 ==========

    /**
     * 简历分析 Stream Key
     */
    public static final String RESUME_ANALYZE_STREAM_KEY = "resume:analyze:stream";

    /**
     * 简历分析 Consumer Group 名称
     */
    public static final String RESUME_ANALYZE_GROUP_NAME = "analyze-group";

    /**
     * 简历分析 Consumer 名称前缀
     */
    public static final String RESUME_ANALYZE_CONSUMER_PREFIX = "analyze-consumer-";

    /**
     * 简历ID字段
     */
    public static final String FIELD_RESUME_ID = "resumeId";

    // ========== 面试评估 Stream 配置 ==========

    /**
     * 面试评估 Stream Key
     */
    public static final String INTERVIEW_EVALUATE_STREAM_KEY = "interview:evaluate:stream";

    /**
     * 面试评估 Consumer Group 名称
     */
    public static final String INTERVIEW_EVALUATE_GROUP_NAME = "evaluate-group";

    /**
     * 面试评估 Consumer 名称前缀
     */
    public static final String INTERVIEW_EVALUATE_CONSUMER_PREFIX = "evaluate-consumer-";

    /**
     * 面试会话ID字段
     */
    public static final String FIELD_SESSION_ID = "sessionId";

    // ========== 语音面试评估 Stream 配置 ==========

    /**
     * 语音面试评估 Stream Key
     */
    public static final String VOICE_EVALUATE_STREAM_KEY = "voice:evaluate:stream";

    /**
     * 语音面试评估 Consumer Group 名称
     */
    public static final String VOICE_EVALUATE_GROUP_NAME = "voice-evaluate-group";

    /**
     * 语音面试评估 Consumer 名称前缀
     */
    public static final String VOICE_EVALUATE_CONSUMER_PREFIX = "voice-evaluate-consumer-";

    /**
     * 语音面试会话ID字段
     */
    public static final String FIELD_VOICE_SESSION_ID = "voiceSessionId";

    // ========== 死信队列（DLQ）配置 ==========

    /**
     * 知识库向量化死信 Stream Key：重试耗尽后写入，保留失败现场供排查/回放
     */
    public static final String KB_VECTORIZE_DLQ_KEY = "knowledgebase:vectorize:dlq";

    /**
     * 简历分析死信 Stream Key
     */
    public static final String RESUME_ANALYZE_DLQ_KEY = "resume:analyze:dlq";

    /**
     * 面试评估死信 Stream Key
     */
    public static final String INTERVIEW_EVALUATE_DLQ_KEY = "interview:evaluate:dlq";

    /**
     * 语音面试评估死信 Stream Key
     */
    public static final String VOICE_EVALUATE_DLQ_KEY = "voice:evaluate:dlq";
}
