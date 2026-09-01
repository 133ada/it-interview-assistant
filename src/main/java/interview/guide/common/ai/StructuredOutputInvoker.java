package interview.guide.common.ai;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import org.slf4j.Logger;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.converter.BeanOutputConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 统一封装结构化输出调用与重试策略。
 * 主要用于解决大模型（LLM）返回非标准 JSON 或未按约定格式返回导致的解析失败问题。
 */
@Component
public class StructuredOutputInvoker {

    // 静态常量定义：用于在重试时强力引导大模型严格输出标准的 JSON 字符串
    private static final String STRICT_JSON_INSTRUCTION = """
                请仅返回可被 JSON 解析器直接解析的 JSON 对象，并严格满足字段结构要求：
                1) 不要输出 Markdown 代码块（如 ```json）。
                2) 不要输出任何解释文字、前后缀、注释。
                3) 所有字符串内引号必须正确转义。
            """;

    // 监控指标（Metrics）相关的 Key 定义
    private static final String METRIC_INVOCATIONS = "app.ai.structured_output.invocations";
    private static final String METRIC_ATTEMPTS = "app.ai.structured_output.attempts";
    private static final String METRIC_LATENCY = "app.ai.structured_output.latency";
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAILURE = "failure";
    private static final int MAX_CONTEXT_TAG_LENGTH = 48;
    private static final Pattern NON_ALNUM_PATTERN = Pattern.compile("[^a-z0-9_]+");
    private static final Pattern MULTI_UNDERSCORE = Pattern.compile("_+");

    // 配置属性变量：通过构造函数从外部配置类映射注入
    private final int maxAttempts;                            // 最大尝试/重试次数
    private final boolean includeLastErrorInRetryPrompt;      // 重试时 Prompt 是否包含上一次的报错信息
    private final boolean retryUseRepairPrompt;               // 是否开启重试 Prompt 修补机制
    private final boolean retryAppendStrictJsonInstruction;   // 重试时是否追加严格的 JSON 指令
    private final int errorMessageMaxLength;                  // 拼入 Prompt 的错误信息的最大长度
    private final boolean metricsEnabled;                     // 是否启用监控指标埋点
    private final MeterRegistry meterRegistry;                // Micrometer 指标注册表（可选注入）


    public StructuredOutputInvoker(
            StructuredOutputProperties properties,
            @Autowired(required = false) MeterRegistry meterRegistry
    ) {
        this.maxAttempts = Math.max(1, properties.getStructuredMaxAttempts());
        this.includeLastErrorInRetryPrompt = properties.isStructuredIncludeLastError();
        this.retryUseRepairPrompt = properties.isStructuredRetryUseRepairPrompt();
        this.retryAppendStrictJsonInstruction = properties.isStructuredRetryAppendStrictJsonInstruction();
        this.errorMessageMaxLength = Math.max(20, properties.getStructuredErrorMessageMaxLength());
        this.metricsEnabled = properties.isStructuredMetricsEnabled();
        this.meterRegistry = meterRegistry;
    }

    /**
     * 核心业务执行方法：负责发送请求给大模型、拦截解析异常并执行重试策略。
     *
     * @param chatClient             Spring AI 的对话客户端
     * @param systemPromptWithFormat 带有结构化格式说明的系统 Prompt
     * @param userPrompt             用户实际输入的 Prompt
     * @param outputConverter        负责将 JSON 映射为 Java 对象的转换器
     * @param errorCode              解析最终失败时需要抛出的业务错误码
     * @param errorPrefix            异常信息的前缀
     * @param logContext             日志和指标上下文标识（如业务场景名）
     * @param log                    外部传入的日志记录器
     * @return 解析成功后的 Java 对象
     */
    public <T> T invoke(
            ChatClient chatClient,
            String systemPromptWithFormat,
            String userPrompt,
            BeanOutputConverter<T> outputConverter,
            ErrorCode errorCode,
            String errorPrefix,
            String logContext,
            Logger log
    ) {
        long startNanos = System.nanoTime(); // 记录开始时间（纳秒），用于计算总耗时
        String contextTag = normalizeContextTag(logContext); // 规范化上下文名称，使其符合监控指标 Tag 规范

        // 在原始系统 Prompt 后面强制拼接防注入、防投毒的安全拦截指令
        String securedSystemPrompt = systemPromptWithFormat
                + PromptSecurityConstants.ANTI_INJECTION_INSTRUCTION;
        Exception lastError = null; // 用于记录上一次循环中抛出的异常

        // 开始重试循环
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            // 如果是第1次调用则使用基础 Prompt；如果是重试，则调用方法动态构建包含错误反馈的重试 Prompt
            String attemptSystemPrompt = attempt == 1
                    ? securedSystemPrompt
                    : buildRetrySystemPrompt(securedSystemPrompt, lastError);
            try {
                // 流式/阻塞调用大模型并获取文本返回内容
                String content = chatClient.prompt()
                        .system(attemptSystemPrompt)
                        .user(userPrompt)
                        .call()
                        .content();
                T result = convertWithRepair(content, outputConverter, logContext, log); // 尝试解析返回的文本（包含本地弱容错修复逻辑）

                // 执行到此说明成功，记录单次尝试成功指标和整体调用成功指标（含耗时）
                recordAttempt(contextTag, STATUS_SUCCESS);
                recordInvocation(contextTag, STATUS_SUCCESS, startNanos);
                return result; // 成功拿到结果，提前返回
            } catch (Exception e) {
                lastError = e;
                recordAttempt(contextTag, STATUS_FAILURE); // 记录单次尝试失败指标

                // 根据当前尝试次数打印不同级别的日志
                if (attempt < maxAttempts) {
                    log.warn("{}结构化解析失败，准备重试: attempt={}/{}, error={}",
                            logContext, attempt, maxAttempts, e.getMessage());
                } else {
                    log.error("{}结构化解析失败，已达最大重试次数: attempts={}, error={}",
                            logContext, maxAttempts, e.getMessage());
                }
            }
        }

        recordInvocation(contextTag, STATUS_FAILURE, startNanos);
        throw new BusinessException(
                errorCode,
                errorPrefix + (lastError != null ? lastError.getMessage() : "unknown")
        );
    }

    /**
     * 具备本地修复能力的转换方法：当默认解析失败时，尝试在本地修正最常见的 JSON 引号未转义错误。
     */
    private <T> T convertWithRepair(
            String content,
            BeanOutputConverter<T> outputConverter,
            String logContext,
            Logger log
    ) {
        try {
            return outputConverter.convert(content); // 首先尝试用原版内容进行常规结构化解析
        } catch (Exception firstError) {
            // 常规解析失败，触发本地文本修复逻辑（主要针对大模型输出的 Value 内部包含未转义双引号的情形）
            String repaired = repairUnescapedQuotesInJsonStrings(content);
            if (!repaired.equals(content)) {
                try {
                    T result = outputConverter.convert(repaired);
                    log.warn("{}结构化 JSON 存在未转义引号，已在本地修复后解析成功", logContext);
                    return result; // 修复后成功解析，返回结果
                } catch (Exception repairError) {
                    // 修复后依然解析失败，则将修复过程中的异常附加到原始异常中
                    firstError.addSuppressed(repairError);
                }
            }
            throw firstError; // 抛出最原始的解析异常（让重试外层知晓）
        }
    }

    /**
     * 核心算法：通过状态机扫描，在本地自动修复 JSON 字符串内部未转义的双引号。
     * 主要修复的问题：大模型在返回结构化数据时，经常会犯一个错误：在 JSON 的字符串内部直接使用双引号，而忘记了加反斜杠 \ 进行转义。
     * 比如：将 {"book_name": "修仙从"删库跑路"开始"}  转义为： {"book_name": "修仙从\"删库跑路\"开始"}
     */
    private String repairUnescapedQuotesInJsonStrings(String content) {
        if (content == null || content.isBlank()) {
            return content;
        }
        StringBuilder repaired = new StringBuilder(content.length() + 16);
        boolean inString = false;
        boolean escaping = false;
        for (int i = 0; i < content.length(); i++) {
            char ch = content.charAt(i);
            if (!inString) {
                if (ch == '"') {
                    inString = true;
                }
                repaired.append(ch);
                continue;
            }

            if (escaping) {
                repaired.append(ch);
                escaping = false;
                continue;
            }
            if (ch == '\\') {
                repaired.append(ch);
                escaping = true;
                continue;
            }
            if (ch == '"') {
                if (isLikelyJsonStringTerminator(content, i + 1)) {
                    inString = false;
                    repaired.append(ch);
                } else {
                    repaired.append("\\\"");
                }
                continue;
            }
            repaired.append(ch);
        }
        return repaired.toString();
    }

    private boolean isLikelyJsonStringTerminator(String content, int start) {
        for (int i = start; i < content.length(); i++) {
            char next = content.charAt(i);
            if (Character.isWhitespace(next)) {
                continue;
            }
            // 合法的 JSON 字符串终止后，后面通常只能是逗号、右花括号、右方括号或冒号（如果是 Key 的话）
            return next == ',' || next == '}' || next == ']' || next == ':';
        }
        return true;
    }

    /**
     * 动态重试 Prompt 构建器
     */
    private String buildRetrySystemPrompt(String systemPromptWithFormat, Exception lastError) {
        if (!retryUseRepairPrompt) {
            return systemPromptWithFormat;
        }

        StringBuilder prompt = new StringBuilder(systemPromptWithFormat)
                .append("\n\n");

        if (retryAppendStrictJsonInstruction) {
            prompt.append(STRICT_JSON_INSTRUCTION).append('\n');
        }
        prompt.append("上次输出解析失败，请仅返回合法 JSON。");

        if (includeLastErrorInRetryPrompt && lastError != null && lastError.getMessage() != null) {
            prompt.append("\n上次失败原因：")
                    .append(sanitizeErrorMessage(lastError.getMessage()));
        }
        return prompt.toString();
    }

    /**
     * 清洗和截断异常提示：防止过长或者带有大量换行格式的错误日志污染大模型的上下文。
     */
    private String sanitizeErrorMessage(String message) {
        String oneLine = message.replace('\n', ' ').replace('\r', ' ').trim();
        if (oneLine.length() > errorMessageMaxLength) {
            return oneLine.substring(0, errorMessageMaxLength) + "...";
        }
        return oneLine;
    }

    /**
     * 指标埋点：记录单次 Attempt 尝试次数
     */
    private void recordAttempt(String contextTag, String status) {
        if (!isMetricsAvailable()) {
            return;
        }
        meterRegistry.counter(
                METRIC_ATTEMPTS,
                Tags.of("context", contextTag, "status", status)
        ).increment();
    }

    /**
     * 指标埋点：记录整体 Invocation 业务调用次数以及耗时时长
     */
    private void recordInvocation(String contextTag, String status, long startNanos) {
        if (!isMetricsAvailable()) {
            return;
        }
        Tags tags = Tags.of("context", contextTag, "status", status);
        meterRegistry.counter(METRIC_INVOCATIONS, tags).increment();
        meterRegistry.timer(METRIC_LATENCY, tags)
                .record(System.nanoTime() - startNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    /**
     * 安全校验：判断监控功能是否可用
     */
    private boolean isMetricsAvailable() {
        return metricsEnabled && meterRegistry != null;
    }

    /**
     * 指标标签规范化：将传入的复杂业务标识，转换成标准的 Prometheus/Micrometer 兼容的蛇形命名法（snake_case）。
     * 规则包括：转小写、空格转下划线、过滤非法字符、合并连续下划线、首尾裁切、控制最大长度。
     */
    private String normalizeContextTag(String raw) {
        String source = (raw == null || raw.isBlank()) ? "unknown" : raw;
        String normalized = source.toLowerCase(Locale.ROOT).trim().replace(' ', '_');
        normalized = NON_ALNUM_PATTERN.matcher(normalized).replaceAll("_");
        normalized = MULTI_UNDERSCORE.matcher(normalized).replaceAll("_");
        normalized = normalized.replaceAll("^_+|_+$", "");
        if (normalized.isBlank()) {
            normalized = "unknown";
        }
        if (normalized.length() > MAX_CONTEXT_TAG_LENGTH) {
            normalized = normalized.substring(0, MAX_CONTEXT_TAG_LENGTH);
        }
        return normalized;
    }
}
