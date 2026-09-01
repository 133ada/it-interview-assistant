package interview.guide.common.ai;

import interview.guide.common.config.LlmProviderProperties;
import interview.guide.common.config.LlmProviderProperties.AdvisorConfig;
import interview.guide.common.config.LlmProviderProperties.ProviderConfig;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.modules.llmprovider.model.LlmGlobalSettingEntity;
import interview.guide.modules.llmprovider.model.LlmProviderEntity;
import interview.guide.modules.llmprovider.repository.LlmGlobalSettingRepository;
import interview.guide.modules.llmprovider.repository.LlmProviderRepository;
import interview.guide.modules.llmprovider.service.ApiKeyEncryptionService;
import io.micrometer.observation.ObservationRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.client.advisor.SafeGuardAdvisor;
import org.springframework.ai.chat.client.advisor.SimpleLoggerAdvisor;
import org.springframework.ai.chat.client.advisor.ToolCallAdvisor;
import org.springframework.ai.chat.client.advisor.api.Advisor;
import org.springframework.ai.document.MetadataMode;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.OpenAiEmbeddingModel;
import org.springframework.ai.openai.OpenAiEmbeddingOptions;
import org.springframework.ai.openai.api.OpenAiApi;
import org.springframework.ai.retry.RetryUtils;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing and caching LLM providers.
 * Supports dynamic creation of ChatClient based on provider configurations.
 * 注册表类：用于动态管理、配置和缓存底层 LLM（大语言模型）提供商。
 * 它能够根据数据库或配置文件中的配置，动态生成 Spring AI 核心的 ChatClient 和 EmbeddingModel。
 */
@Component
@Slf4j
public class LlmProviderRegistry {

    private final LlmProviderProperties properties; // 声明配置属性类，用于读取 application.yml 中的默认 LLM 参数
    private final Map<String, ChatClient> clientCache = new ConcurrentHashMap<>(); // 线程安全的 ConcurrentHashMap , 缓存聊天模型
    private final Map<String, EmbeddingModel> embeddingModelCache = new ConcurrentHashMap<>(); // 线程安全的 ConcurrentHashMap , 缓存向量模型

    // 注入数据库仓库，用于支持从后台管理页面动态读取模型和 API Key 的配置
    private final LlmProviderRepository providerRepository;
    private final LlmGlobalSettingRepository globalSettingRepository;

    private final ApiKeyEncryptionService encryptionService; // 提供加密、解密

    private final ToolCallingManager toolCallingManager; // Spring AI 的工具调用管理器，负责协调模型对外部系统工具（Function Calling）的调用
    private final ObservationRegistry observationRegistry; // 用于监控和度量指标收集的注册表，跟踪 AI 调用的耗时与状态
    private final ToolCallback interviewSkillsToolCallback; // 专属的面试技能工具回调，封装了具体的业务工具（例如查询岗位信息、提取关键词等）

    // 定义常见厂商的推荐 Embedding（向量化）模型映射表，当用户配置错误时用于友好提示
    private static final Map<String, String> RECOMMENDED_EMBEDDING_MODELS = Map.of(
            "dashscope", "text-embedding-v3",
            "glm", "embedding-3",
            "zhipu", "embedding-3",
            "baidu", "Embedding-V1",
            "minimax", "embo-01"
    );

    /**
     * 主构造函数：由 Spring 自动注入所需的所有依赖项。
     * 部分依赖标记为 `required = false` 或配有 `@Qualifier`，增强了框架的容错性。
     */
    @Autowired
    public LlmProviderRegistry(
            LlmProviderProperties properties,
            LlmProviderRepository providerRepository,
            LlmGlobalSettingRepository globalSettingRepository,
            ApiKeyEncryptionService encryptionService,
            @Autowired(required = false) ToolCallingManager toolCallingManager,
            @Autowired(required = false) ObservationRegistry observationRegistry,
            @Autowired(required = false) @Qualifier("interviewSkillsToolCallback") ToolCallback interviewSkillsToolCallback) {
        this.properties = properties;
        this.providerRepository = providerRepository;
        this.globalSettingRepository = globalSettingRepository;
        this.encryptionService = encryptionService;
        this.toolCallingManager = toolCallingManager;
        this.observationRegistry = observationRegistry;
        this.interviewSkillsToolCallback = interviewSkillsToolCallback;
    }

    public LlmProviderRegistry(
            LlmProviderProperties properties,
            ToolCallingManager toolCallingManager,
            ObservationRegistry observationRegistry,
            ToolCallback interviewSkillsToolCallback) {
        this(properties, null, null, null, toolCallingManager, observationRegistry, interviewSkillsToolCallback);
    }

    /**
     * Get a ChatClient for the specified provider ID.
     * If the client is not in the cache, it will be created based on the provider's configuration.
     * 获取指定提供商ID的聊天客户端。
     * 如果客户端不在缓存中，将根据提供者的配置创建。
     *
     * @param providerId The ID of the provider (e.g., "dashscope", "lmstudio")
     * @return A ChatClient instance
     * @throws IllegalArgumentException if the providerId is unknown
     */
    public ChatClient getChatClient(String providerId) {
        return clientCache.computeIfAbsent(providerId, id -> {
            log.info("[LlmProviderRegistry] Creating new client for provider: {}", id);
            return createChatClient(id);
        });
    }

    /**
     * Get the default ChatClient based on app.ai.default-provider.
     * 根据 app.ai.default-provider 获取默认的 ChatClient。
     *
     * @return The default ChatClient instance
     */
    public ChatClient getDefaultChatClient() {
        return getChatClient(resolveDefaultChatProviderId());
    }

    /**
     * Get a ChatClient for the specified provider, falling back to the default if null or blank.
     * 获取指定提供程序的 ChatClient，如果为 null 或为空则回退到默认值。
     */
    public ChatClient getChatClientOrDefault(String providerId) {
        if (providerId != null && !providerId.isBlank()) {
            return getChatClient(providerId);
        }
        return getDefaultChatClient();
    }

    /**
     * 获取不带 SkillsTool 的 ChatClient，用于结构化输出场景（出题、简历评分等）。
     * 这些场景要求模型一次性返回可解析 JSON，不应混入工具调用消息。
     */
    public ChatClient getPlainChatClient(String providerId) {
        String id = resolveProviderId(providerId);
        return clientCache.computeIfAbsent(id + ":plain", key -> createPlainChatClient(id));
    }

    /**
     * 获取语音面试专用 ChatClient：SkillsTool + ToolCallAdvisor（流式）。
     * 不加 Memory Advisor（语音面试手动管理对话历史）。
     */
    public ChatClient getVoiceChatClient(String providerId) {
        String id = resolveProviderId(providerId);
        return clientCache.computeIfAbsent(id + ":voice", key -> createVoiceChatClient(id));
    }

    /**
     * 清空缓存，重新加载所有 provider。
     */
    public void reload() {
        int size = clientCache.size() + embeddingModelCache.size();
        clientCache.clear();
        embeddingModelCache.clear();
        log.info("[LlmProviderRegistry] Cache cleared ({} entries). Next access will re-create clients.", size);
    }

    public EmbeddingModel getEmbeddingModel(String providerId) {
        return embeddingModelCache.computeIfAbsent(providerId, id -> {
            log.info("[LlmProviderRegistry] Creating new embedding model for provider: {}", id);
            return createEmbeddingModel(id);
        });
    }

    public EmbeddingModel getDefaultEmbeddingModel() {
        return getEmbeddingModel(resolveDefaultEmbeddingProviderId());
    }

    // 有工具（interviewSkillsToolCallback、） + 有记忆
    private ChatClient createChatClient(String providerId) {
        OpenAiChatModel chatModel = buildChatModel(providerId); // 创建 chatModel ，因为它才是真正负责和大模型通信的对象。

        ChatClient.Builder builder = ChatClient.builder(chatModel); // 创建 Builder
        if (interviewSkillsToolCallback != null) {
            builder.defaultToolCallbacks(interviewSkillsToolCallback); //chatModel 具有调用外部的 skills 的功能
        }
        List<Advisor> advisors = buildDefaultAdvisors(providerId); // 构建默认的拦截器/通知器链（如日志、记忆、安全防护）
        if (!advisors.isEmpty()) {
            builder.defaultAdvisors(advisors.toArray(new Advisor[0])); // 将拦截器数组绑定到客户端上
            log.info("[LlmProviderRegistry] Applied {} advisors for provider {}", advisors.size(), providerId);
        }

        return builder.build(); // 正式生成面向业务的标准 ChatClient 实例
    }

    // 无工具 ，用于： JSON输出、出题、评分
    private ChatClient createPlainChatClient(String providerId) {
        OpenAiChatModel chatModel = buildChatModel(providerId);
        ChatClient.Builder builder = ChatClient.builder(chatModel);
        buildSafeGuardAdvisor().ifPresent(advisor -> builder.defaultAdvisors(advisor));
        log.info("[LlmProviderRegistry] Created plain ChatClient (no tools) for {}", providerId);
        return builder.build();
    }

    //  有工具、 流式调用、 没有 memory
    private ChatClient createVoiceChatClient(String providerId) {
        OpenAiChatModel chatModel = buildChatModel(providerId);

        ChatClient.Builder builder = ChatClient.builder(chatModel);
        if (interviewSkillsToolCallback != null) {
            builder.defaultToolCallbacks(interviewSkillsToolCallback);
        }
        List<Advisor> advisors = new ArrayList<>();
        if (toolCallingManager != null) {
            advisors.add(buildToolCallAdvisor(true, true));
        }
        buildSafeGuardAdvisor().ifPresent(advisors::add);
        if (!advisors.isEmpty()) {
            builder.defaultAdvisors(advisors.toArray(new Advisor[0]));
        }
        log.info("[LlmProviderRegistry] Created voice ChatClient (SkillsTool + streaming ToolCall) for {}", providerId);
        return builder.build();
    }

    /**
     * 构筑底层的 OpenAiChatModel 实例。
     * 因为目前主流大模型（如 DeepSeek、零一万物、阿里 Qwen、智谱等）均全面兼容 OpenAI 的 API 协议规范，
     * 所以这里统一采用 Spring AI 提供的 `OpenAiChatModel` 进行适配。
     */
    private OpenAiChatModel buildChatModel(String providerId) {
        ProviderSnapshot config = loadProviderOrThrow(providerId); // 获取当前渠道的配置快照（包含 URL、解密后的 Key、模型名等）
        log.info("[LlmProviderRegistry] Building ChatModel - Provider: {}, BaseUrl: {}, Model: {}",
                providerId, config.baseUrl(), config.model());

        // 利用工具类构建标准 OpenAI 客户端底座
        OpenAiApi openAiApi = ApiPathResolver.buildOpenAiApi(config.baseUrl(), config.apiKey());

        // 设置模型参数。若后台配置了 temperature（多样性/随机度）则使用配置值，否则默认 0.2（偏向严谨、确定性的面试场景）
        OpenAiChatOptions options = OpenAiChatOptions.builder()
                .model(config.model())
                .temperature(config.temperature() != null ? config.temperature() : 0.2)
                .build();

        // 实例化 OpenAiChatModel，并注入重试模板和监控指标注册表
        return new OpenAiChatModel(
                openAiApi,
                options,
                toolCallingManager,
                RetryUtils.DEFAULT_RETRY_TEMPLATE,
                observationRegistry != null ? observationRegistry : ObservationRegistry.NOOP
        );
    }

    /**
     * 创建底层的向量化模型（EmbeddingModel）。
     * 用于把知识库、题库、简历片段转化为向量数据，以便进行 RAG（检索增强生成）搜索。
     */
    private EmbeddingModel createEmbeddingModel(String providerId) {
        ProviderSnapshot config = loadProviderOrThrow(providerId); //  获取配置快照

        // 校验：若配置明确指出不支持 Embedding，或者模型名称为空，则直接拦截并抛出异常
        if (!config.supportsEmbedding() || isBlank(config.embeddingModel())) {
            throw new BusinessException(ErrorCode.PROVIDER_CONFIG_READ_FAILED,
                    "Provider '" + providerId + "' 未配置可用的 Embedding 模型，无法执行知识库向量化");
        }

        // 防错机制：防止管理员误把聊天模型的名字（如 qwen-turbo）填到了向量模型输入框中
        if (looksLikeChatModel(config.embeddingModel())) {
            String recommendation = RECOMMENDED_EMBEDDING_MODELS.get(providerId.toLowerCase());
            String suffix = recommendation != null
                    ? "，推荐填写 " + recommendation
                    : "，请填写该厂商真实的 Embedding 模型名";
            throw new BusinessException(ErrorCode.PROVIDER_CONFIG_READ_FAILED,
                    "Provider '" + providerId + "' 的 Embedding Model 配成了聊天模型 '"
                            + config.embeddingModel() + "'" + suffix);
        }
        log.info("[LlmProviderRegistry] Building EmbeddingModel - Provider: {}, BaseUrl: {}, Model: {}",
                providerId, config.baseUrl(), config.embeddingModel());

        // 构建 OpenAI 协议底座和配置项
        OpenAiApi openAiApi = ApiPathResolver.buildOpenAiApi(config.baseUrl(), config.apiKey());
        OpenAiEmbeddingOptions options = OpenAiEmbeddingOptions.builder()
                .model(config.embeddingModel())
                .dimensions(resolveEmbeddingDimensions(config.embeddingDimensions()))
                .build();

        // 实例化 Spring AI 的 OpenAiEmbeddingModel
        return new OpenAiEmbeddingModel(
                openAiApi,
                MetadataMode.EMBED, // 指定在向量化时，元数据的处理模式
                options,
                RetryUtils.DEFAULT_RETRY_TEMPLATE,
                observationRegistry != null ? observationRegistry : ObservationRegistry.NOOP
        );
    }

    /**
     * 根据全局配置，拼装标准 ChatClient 所需的默认 Advisor（拦截器/通知器）列表。
     */
    private List<Advisor> buildDefaultAdvisors(String providerId) {
        AdvisorConfig config = properties.getAdvisors();
        if (config == null || !config.isEnabled()) {
            return List.of();
        }

        List<Advisor> advisors = new ArrayList<>();

        // 装配工具调用 Advisor：负责在模型决定调用工具时拦截并代为执行
        if (config.isToolCallEnabled()) {
            if (toolCallingManager != null) {
                advisors.add(buildToolCallAdvisor(
                        config.isToolCallConversationHistoryEnabled(),
                        config.isStreamToolCallResponses()));
            } else {
                log.warn("[LlmProviderRegistry] ToolCallAdvisor skipped: ToolCallingManager unavailable, provider={}", providerId);
            }
        }

        // 装配滑动窗口聊天记忆 Advisor：让大模型天然具备多轮对话的记忆能力
        if (config.isMessageChatMemoryEnabled()) {
            int maxMessages = Math.max(20, config.getMessageChatMemoryMaxMessages());
            MessageChatMemoryAdvisor memoryAdvisor = MessageChatMemoryAdvisor.builder(
                    MessageWindowChatMemory.builder()
                            .maxMessages(maxMessages) // 超出此数量的消息会自动滚动滑出窗口，防止上下文爆 Token
                            .build()
            ).build();
            advisors.add(memoryAdvisor);
        }

        // 装配标准日志 Advisor：在控制台友好地打印出每次请求大模型的 Prompt 和返回的 Response
        if (config.isSimpleLoggerEnabled()) {
            advisors.add(new SimpleLoggerAdvisor());
        }

        buildSafeGuardAdvisor().ifPresent(advisors::add);

        return advisors;
    }

    /**
     * 构建工具调用拦截器的具体工厂方法。
     */
    private ToolCallAdvisor buildToolCallAdvisor(boolean conversationHistoryEnabled,
                                                 boolean streamToolCallResponses) {
        return ToolCallAdvisor.builder()
                .toolCallingManager(toolCallingManager)
                .conversationHistoryEnabled(conversationHistoryEnabled)
                .streamToolCallResponses(streamToolCallResponses)
                .build();
    }

    /**
     * 构建内容护栏/合规性安全拦截器（SafeGuardAdvisor）。
     * 当检测到敏感词时，直接拦截请求并给出系统预设的委婉回应，无需浪费 Token 请求大模型。
     */
    private Optional<SafeGuardAdvisor> buildSafeGuardAdvisor() {
        AdvisorConfig config = properties.getAdvisors();
        if (config == null || !config.isSafeguardEnabled()) {
            return Optional.empty();
        }
        SafeGuardAdvisor advisor = SafeGuardAdvisor.builder()
                .sensitiveWords(config.getSafeguardWords()) // 载入预设的敏感词词典
                .failureResponse("抱歉，我只能协助面试相关的任务。") // 被拦截时的兜底回复语
                .order(100) // 设定拦截器的优先级顺序
                .build();
        return Optional.of(advisor);
    }

    /**
     * 解析提供商 ID 的辅助方法：若传入的 ID 无效，则自动将其修正、规整为默认提供商 ID。
     */
    private String resolveProviderId(String providerId) {
        return (providerId != null && !providerId.isBlank())
                ? providerId : resolveDefaultChatProviderId();
    }

    /**
     * 解析默认的【对话模型】提供商 ID。
     * 策略：优先查数据库中的全局设置单例（ID=1），若数据库没有或未配置，则拿配置文件 yml 的兜底配置。
     */
    private String resolveDefaultChatProviderId() {
        if (globalSettingRepository == null) {
            return properties.getDefaultProvider();
        }
        return globalSettingRepository.findById(LlmGlobalSettingEntity.SINGLETON_ID)
                .map(LlmGlobalSettingEntity::getDefaultChatProviderId)
                .filter(id -> !isBlank(id))
                .orElse(properties.getDefaultProvider());
    }

    /**
     * 解析默认的【向量模型】提供商 ID。
     * 策略：查数据库配置 -> 查 yml 中专属的 defaultEmbeddingProvider -> 查 yml 中的 defaultProvider 兜底。
     */
    private String resolveDefaultEmbeddingProviderId() {
        if (globalSettingRepository == null) {
            return !isBlank(properties.getDefaultEmbeddingProvider())
                    ? properties.getDefaultEmbeddingProvider()
                    : properties.getDefaultProvider();
        }
        return globalSettingRepository.findById(LlmGlobalSettingEntity.SINGLETON_ID)
                .map(LlmGlobalSettingEntity::getDefaultEmbeddingProviderId)
                .filter(id -> !isBlank(id))
                .orElseGet(() -> !isBlank(properties.getDefaultEmbeddingProvider())
                        ? properties.getDefaultEmbeddingProvider()
                        : properties.getDefaultProvider());
    }

    /**
     * 统一加载模型渠道配置信息（核心路由机制）。
     * 策略：优先从数据库中读取启用的渠道实体并利用 Service 解密 API Key。若数据库组件未注入，则回退到 yml 文件中读取。
     */
    private ProviderSnapshot loadProviderOrThrow(String providerId) {
        if (providerRepository == null) {
            return loadProviderFromPropertiesOrThrow(providerId);
        }

        // 从数据库查询对应的渠道记录，并校验该渠道在后台是否被禁用
        LlmProviderEntity entity = providerRepository.findById(providerId)
                .filter(LlmProviderEntity::isEnabled)
                .orElseThrow(() -> new IllegalArgumentException("Unknown LLM provider: " + providerId));

        // 返回包含解密后明文 API Key 的不可变快照对象 (Record)
        return new ProviderSnapshot(
                entity.getId(),
                entity.getBaseUrl(),
                encryptionService.decrypt(entity.getApiKeyNonce(), entity.getApiKeyCiphertext()),
                entity.getModel(),
                entity.getEmbeddingModel(),
                entity.getEmbeddingDimensions(),
                entity.isSupportsEmbedding(),
                entity.getTemperature()
        );
    }

    /**
     * 当没有连接数据库时，降级从系统的 application.yml 配置文件中加载渠道参数。
     */
    private ProviderSnapshot loadProviderFromPropertiesOrThrow(String providerId) {
        ProviderConfig config = properties.getProviders().get(providerId);
        if (config == null) {
            log.error("[LlmProviderRegistry] Provider config not found: {}", providerId);
            throw new IllegalArgumentException("Unknown LLM provider: " + providerId);
        }
        boolean supportsEmbedding = Boolean.TRUE.equals(config.getSupportsEmbedding())
                || !isBlank(config.getEmbeddingModel());
        return new ProviderSnapshot(
                providerId,
                config.getBaseUrl(),
                config.getApiKey(),
                config.getModel(),
                config.getEmbeddingModel(),
                config.getEmbeddingDimensions(),
                supportsEmbedding,
                config.getTemperature()
        );
    }

    /**
     * 内部字符串非空判断辅助函数。
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * 确定向量的生成维度。如果当前渠道指定了维度则使用渠道值，否则采用系统的全局默认维度（如 1536）。
     */
    private Integer resolveEmbeddingDimensions(Integer configuredDimensions) {
        if (configuredDimensions != null && configuredDimensions > 0) {
            return configuredDimensions;
        }
        return properties.getEmbeddingDimensions();
    }

    /**
     * 启发式前缀匹配：判断输入的模型字符串是否“看起来像是一个聊天对话模型”。
     * 用于在配置 Embedding Model 时进行强力防错校验，拦截常见的各类国产/开源大模型聊天名。
     */
    private boolean looksLikeChatModel(String model) {
        String lower = model.toLowerCase();
        return lower.startsWith("glm-")
                || lower.startsWith("deepseek")
                || lower.startsWith("kimi")
                || lower.startsWith("moonshot")
                || lower.startsWith("qwen")
                || lower.startsWith("ernie");
    }

    /**
     * 内部不可变数据载体（Java 16+ Record）。
     * 用于打包渠道的所有运行时配置，解耦复杂的配置来源，为模型生产线提供干净的数据源。
     */
    private record ProviderSnapshot(
            String id,
            String baseUrl,
            String apiKey,
            String model,
            String embeddingModel,
            Integer embeddingDimensions,
            boolean supportsEmbedding,
            Double temperature
    ) {
    }
}
