package interview.guide.modules.interview.model;

import interview.guide.modules.interview.skill.InterviewSkillService.CategoryDTO;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.List;

/**
 * 创建面试会话请求
 */
@Schema(name = "InterviewCreateSessionRequest")
public record CreateInterviewRequest(
    @Schema(description = "简历文本内容（可选，无简历时为通用面试）", example = "3年Java后端经验，熟悉Spring生态")
    String resumeText,      // 简历文本内容（可选，无简历时为通用面试）

    @Schema(description = "面试题目数量 (3-20)", example = "10")
    @Min(value = 3, message = "题目数量最少3题")
    @Max(value = 20, message = "题目数量最多20题")
    int questionCount,      // 面试题目数量 (3-20)

    @Schema(description = "简历ID（可选，无简历时不传）", example = "1")
    Long resumeId,          // 简历ID（可选，无简历时不传）

    @Schema(description = "是否强制创建新会话（忽略未完成的会话），默认为 false", example = "false")
    Boolean forceCreate,    // 是否强制创建新会话（忽略未完成的会话），默认为 false

    @Schema(description = "LLM提供商", example = "default")
    String llmProvider,     // LLM提供商

    @Schema(description = "面试主题 ID（如 java-backend, frontend, custom 等）", example = "java-backend")
    @NotBlank(message = "面试主题不能为空")
    String skillId,         // 面试主题 ID（如 java-backend, frontend, custom 等）

    @Schema(description = "难度级别: junior / mid / senior", example = "mid")
    String difficulty,      // 难度级别: junior / mid / senior

    @Schema(description = "自定义面试的分类（JD 解析结果）")
    List<CategoryDTO> customCategories,   // 自定义面试的分类（JD 解析结果）

    @Schema(description = "JD 原文（自定义面试时作为出题依据）", example = "负责高并发订单系统的设计与开发")
    String jdText                          // JD 原文（自定义面试时作为出题依据）
) {}
