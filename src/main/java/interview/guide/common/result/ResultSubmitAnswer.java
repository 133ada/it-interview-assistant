package interview.guide.common.result;

import interview.guide.modules.interview.model.SubmitAnswerResponse;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "提交答案响应")
public record ResultSubmitAnswer(
    @Schema(description = "状态码", example = "0")
    Integer code,

    @Schema(description = "提示信息", example = "success")
    String message,

    @Schema(description = "答题结果数据")
    SubmitAnswerResponse data,

    @Schema(description = "是否成功", example = "true")
    Boolean success
) {}
