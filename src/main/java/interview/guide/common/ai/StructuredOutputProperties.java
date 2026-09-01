package interview.guide.common.ai;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app.ai")
public class StructuredOutputProperties {

    private int structuredMaxAttempts = 2; // 结构化输出重试次数（用于 BeanOutputConverter 解析失败时的业务层重试）
    private boolean structuredIncludeLastError = true; // 重试时是否将上次错误原因注入提示词，帮助模型定向修复输出
    private boolean structuredRetryUseRepairPrompt = true; // 是否启用“修复型重试提示词”（关闭后每次重试沿用原始 system prompt）
    private boolean structuredRetryAppendStrictJsonInstruction = true; // 修复型重试提示词中是否追加严格 JSON 指令
    private int structuredErrorMessageMaxLength = 200; // 注入到重试提示词中的错误信息最大长度
    private boolean structuredMetricsEnabled = true; // 是否开启结构化输出调用指标（invocations/attempts/latency）
}
