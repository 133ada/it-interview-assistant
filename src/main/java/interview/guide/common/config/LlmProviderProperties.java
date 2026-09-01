package interview.guide.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "app.ai") // 这个Bean不是普通Bean，而是需要从配置文件读取数据。
// default-provider 会转化为 defaultProvider
public class LlmProviderProperties {
    private String defaultProvider = "dashscope";
    private String defaultEmbeddingProvider;
    private Integer embeddingDimensions = 1024;
    private Map<String, ProviderConfig> providers;
    private AdvisorConfig advisors = new AdvisorConfig();
    private String configYamlPath;
    private String configEnvPath;
    private SecurityConfig security = new SecurityConfig();

    @Data
    public static class ProviderConfig {
        private String baseUrl;
        private String apiKey;
        private String model;
        private String embeddingModel;
        private Integer embeddingDimensions;
        private Boolean supportsEmbedding;
        private Double temperature;
    }

    @Data
    public static class SecurityConfig {
        private String apiKeyEncryptionKey;
        private boolean requireEncryptionKey = false;
    }

    @Data
    public static class AdvisorConfig {
        private boolean enabled = true; // 总控开关

        // ToolCallAdvisor
        private boolean toolCallEnabled = true;
        private boolean toolCallConversationHistoryEnabled = false;
        private boolean streamToolCallResponses = false;

        // MessageChatMemoryAdvisor（默认关闭，避免会话串扰）
        private boolean messageChatMemoryEnabled = false;
        private int messageChatMemoryMaxMessages = 120;

        // SimpleLoggerAdvisor（默认关闭）
        private boolean simpleLoggerEnabled = false;

        // SafeGuardAdvisor
        private boolean safeguardEnabled = true;
        private List<String> safeguardWords = List.of(
            "I'll now act as",
            "Sure, I'll ignore",
            "我已经忽略",
            "新的角色是",
            "忽略之前的指令",
            "forget all previous instructions"
        );

        // PromptSanitizer
        private boolean promptSanitizerEnabled = true;
    }
}
