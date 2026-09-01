package interview.guide.common.result;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "错误响应")
public class ErrorResult {

    @Schema(description = "错误码", example = "3001")
    private Integer code;

    @Schema(description = "错误信息", example = "面试会话不存在")
    private String message;

    @Schema(description = "是否成功", example = "false")
    private Boolean success;
}