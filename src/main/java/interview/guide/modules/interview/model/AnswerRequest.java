package interview.guide.modules.interview.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 答案请求体（提交答案 / 暂存答案共用）。
 * sessionId 通过路径变量传入，不在此请求体中。
 */
public record AnswerRequest(
    @Schema(description = "问题索引（从 0 开始）", example = "0")
    @NotNull(message = "问题索引不能为空")
    @Min(value = 0, message = "问题索引无效")
    Integer questionIndex,

    @Schema(description = "用户答案内容", example = "HashMap 底层采用数组+链表+红黑树实现，链表长度超过 8 时转红黑树...")
    @NotBlank(message = "答案不能为空")
    String answer
) {}
