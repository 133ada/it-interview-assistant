package interview.guide.common.result;

import interview.guide.modules.interview.model.InterviewDetailDTO;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "面试详情响应")
public record ResultInterviewDetail(
    @Schema(description = "状态码", example = "0")
    Integer code,

    @Schema(description = "提示信息", example = "success")
    String message,

    @Schema(description = "面试详情数据")
    InterviewDetailDTO data,

    @Schema(description = "是否成功", example = "true")
    Boolean success
) {}
