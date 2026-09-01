package interview.guide.modules.interview.model;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * 获取当前题目响应。
 * 所有题目答完时 completed=true 且携带 message；否则 completed=false 且携带当前题目 question。
 */
public record CurrentQuestionResponse(
    @Schema(description = "是否已答完全部题目", example = "false")
    boolean completed,

    @Schema(description = "完成提示消息（completed=true 时返回）", example = "所有问题已回答完毕")
    String message,

    @Schema(description = "当前题目（completed=false 时返回）")
    InterviewQuestionDTO question
) {}
