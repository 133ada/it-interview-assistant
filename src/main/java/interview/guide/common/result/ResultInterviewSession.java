package interview.guide.common.result;

import interview.guide.modules.interview.model.InterviewSessionDTO;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "面试会话响应")
public record ResultInterviewSession(
    @Schema(description = "状态码", example = "0")
    Integer code,

    @Schema(description = "提示信息", example = "success")
    String message,

    @Schema(description = "返回数据")
    InterviewSessionDTO data,

    @Schema(description = "是否成功", example = "true")
    Boolean success
) {}
