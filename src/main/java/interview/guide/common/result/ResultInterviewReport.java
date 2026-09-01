package interview.guide.common.result;

import interview.guide.modules.interview.model.InterviewReportDTO;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "面试报告响应")
public record ResultInterviewReport(
    @Schema(description = "状态码", example = "0")
    Integer code,

    @Schema(description = "提示信息", example = "success")
    String message,

    @Schema(description = "面试评估报告数据")
    InterviewReportDTO data,

    @Schema(description = "是否成功", example = "true")
    Boolean success
) {}
