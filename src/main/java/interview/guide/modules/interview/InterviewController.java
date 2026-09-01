package interview.guide.modules.interview;

import interview.guide.common.annotation.RateLimit;
import interview.guide.common.result.ErrorResult;
import interview.guide.common.result.Result;
import interview.guide.common.result.ResultInterviewDetail;
import interview.guide.common.result.ResultInterviewReport;
import interview.guide.common.result.ResultInterviewSession;
import interview.guide.common.result.ResultSubmitAnswer;
import interview.guide.modules.interview.model.AnswerRequest;
import interview.guide.modules.interview.model.CreateInterviewRequest;
import interview.guide.modules.interview.model.CurrentQuestionResponse;
import interview.guide.modules.interview.model.InterviewDetailDTO;
import interview.guide.modules.interview.model.InterviewReportDTO;
import interview.guide.modules.interview.model.InterviewSessionDTO;
import interview.guide.modules.interview.model.SessionListItemDTO;
import interview.guide.modules.interview.model.SubmitAnswerRequest;
import interview.guide.modules.interview.model.SubmitAnswerResponse;
import interview.guide.modules.interview.service.InterviewHistoryService;
import interview.guide.modules.interview.service.InterviewPersistenceService;
import interview.guide.modules.interview.service.InterviewSessionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 面试控制器 —— 提供模拟面试相关的 RESTful API 接口
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@Tag(name = "模拟面试", description = "面试会话创建、问答交互与报告生成")
public class InterviewController {

    private final InterviewSessionService sessionService;
    private final InterviewHistoryService historyService;
    private final InterviewPersistenceService persistenceService;

    @Operation(summary = "列出所有面试会话", description = "按创建时间倒序返回所有面试会话摘要列表，用于面试记录列表页，" +
            "每个会话仅包含摘要信息（不含题目列表和答案详情）")
    @GetMapping("/api/interview/sessions")
    public Result<List<SessionListItemDTO>> listSessions() {
        List<SessionListItemDTO> items = persistenceService.findAll().stream()
                .map(SessionListItemDTO::from)
                .toList();
        return Result.success(items);
    }

    @Operation(summary = "创建面试会话",
            description = "根据题目数量、难度、简历信息和面试主题创建新的模拟面试会话。"
                    + "同一简历默认复用未完成会话（断线重连），设 forceCreate=true 强制新建。"
                    + "返回的 sessionId 为 16 位十六进制字符串，status 为 CREATED" +
                    "持久化采用 Cache-Aside：先写 MySQL（真相源），成功后删除 Redis 缓存，首次读取时自动回填。")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "创建成功",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ResultInterviewSession.class))
            ),
            @ApiResponse(responseCode = "3008", description = "面试数据序列化失败",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResult.class))
            ),
            @ApiResponse(responseCode = "3009", description = "面试会话创建失败，请稍后重试",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResult.class))
            ),
            @ApiResponse(responseCode = "3010", description = "面试数据持久化失败，请稍后重试",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = ErrorResult.class))
            ),
    })
    @PostMapping("/api/interview/sessions")
    @RateLimit(dimension = RateLimit.Dimension.GLOBAL, count = 5)
    @RateLimit(dimension = RateLimit.Dimension.IP, count = 5)
    public Result<InterviewSessionDTO> createSession(@Valid @RequestBody CreateInterviewRequest request) {
        log.info("创建面试会话，题目数量: {}", request.questionCount());
        InterviewSessionDTO session = sessionService.createSession(request);
        return Result.success(session);
    }

    @Operation(summary = "获取会话信息",
            description = "根据 sessionId 获取面试会话完整信息，包括题目列表、当前进度和状态。"
                    + "读取策略：Redis 缓存优先，未命中则从 MySQL 恢复并回填缓存")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "请求成功",
                    content = @Content(schema = @Schema(implementation = ResultInterviewSession.class))),
            @ApiResponse(responseCode = "3001", description = "面试会话不存在",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class))),
    })
    @GetMapping("/api/interview/sessions/{sessionId}")
    public Result<InterviewSessionDTO> getSession(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId) {
        InterviewSessionDTO session = sessionService.getSession(sessionId);
        return Result.success(session);
    }

    @Operation(summary = "获取当前题目",
            description = "获取指定会话当前正在作答的题目。首次调用时自动将会话状态从 CREATED 推进为"
                    + "IN_PROGRESS。所有题目答完时返回 completed=true 及提示消息，并且会调用异步任务来评估面试答题详情")
    @GetMapping("/api/interview/sessions/{sessionId}/question")
    public Result<CurrentQuestionResponse> getCurrentQuestion(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId) {
        return Result.success(sessionService.getCurrentQuestionResponse(sessionId));
    }

    @Operation(summary = "提交答案并进入下一题",
            description = "提交当前题目的答案，自动前进到下一题。"
                    + "请求体需传 questionIndex（Integer，从 0 开始）和 answer（String）。"
                    + "最后一题提交后自动触发异步评估任务")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "提交成功",
                    content = @Content(schema = @Schema(implementation = ResultSubmitAnswer.class))),
            @ApiResponse(responseCode = "8001", description = "触发限流，返回错误码 8001（全局10次/窗口）",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class)))
    })
    @PostMapping("/api/interview/sessions/{sessionId}/answers")
    @RateLimit(dimension = RateLimit.Dimension.GLOBAL, count = 10)
    public Result<SubmitAnswerResponse> submitAnswer(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId,
            @Valid @RequestBody AnswerRequest request) {
        log.info("提交答案: 会话{}, 问题{}", sessionId, request.questionIndex());
        SubmitAnswerRequest svcRequest = new SubmitAnswerRequest(sessionId, request.questionIndex(), request.answer());
        SubmitAnswerResponse response = sessionService.submitAnswer(svcRequest);
        return Result.success(response);
    }

    @Operation(summary = "暂存答案",
            description = "保存当前题目的答案但不进入下一题，可重复暂存同一题覆盖之前的答案。"
                    + "请求体需传 questionIndex（Integer）和 answer（String）。"
                    + "首次暂存时自动将会话状态从 CREATED 推进为 IN_PROGRESS")
    @ApiResponses({
            @ApiResponse(responseCode = "3003", description = "面试问题不存在，无效的面试问题索引",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class)))
    })
    @PutMapping("/api/interview/sessions/{sessionId}/answers")
    public Result<Void> saveAnswer(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId,
            @Valid @RequestBody AnswerRequest request) {
        log.info("暂存答案: 会话{}, 问题{}", sessionId, request.questionIndex());
        SubmitAnswerRequest svcRequest = new SubmitAnswerRequest(sessionId, request.questionIndex(), request.answer());
        sessionService.saveAnswer(svcRequest);
        return Result.success(null);
    }

    @Operation(summary = "提前交卷",
            description = "在未答完所有题目时主动结束面试。幂等保护：已交卷或已评估的会话重复调用，交卷后自动触发异步评估" +
                    "交卷后在同一个数据库事务内原子完成：status → COMPLETED + evaluateStatus → PENDING，" +
                    "然后通过 Redis Stream 发送异步评估任务。这里会涉及到 Redis Stream 这种数据类型，来实现消息队列 MQ")
    @ApiResponses({
            @ApiResponse(responseCode = "3004", description = "面试已完成，无效的面试提交任务",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class)))
    })
    @PostMapping("/api/interview/sessions/{sessionId}/complete")
    public Result<Void> completeInterview(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId) {
        log.info("提前交卷: {}", sessionId);
        sessionService.completeInterview(sessionId);
        return Result.success(null);
    }

    @Operation(summary = "生成面试报告",
            description = "对该面试会话的全部答题情况进行 AI 评估，返回包含总分（0-100）、类别得分、"
                    + "逐题反馈、优势与改进建议、参考答案的完整报告。使用 CAS 抢占防止并发重复评估。" +
                    "评估完成后先写 MySQL（status → EVALUATED + 报告落库），再删除 Redis 缓存，下次读取自动回填。")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "生成面试报告成功",
                    content = @Content(schema = @Schema(implementation = ResultInterviewReport.class))),
            @ApiResponse(responseCode = "3004", description = "面试已完成，无效的面试提交任务",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class))),
            @ApiResponse(responseCode = "3005", description = "面试评估失败",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class))),
            @ApiResponse(responseCode = "3007", description = "面试尚未完成",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class)))
    })
    @GetMapping("/api/interview/sessions/{sessionId}/report")
    public Result<InterviewReportDTO> getReport(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId) {
        log.info("生成面试报告: {}", sessionId);
        InterviewReportDTO report = sessionService.generateReport(sessionId);
        return Result.success(report);
    }

    @Operation(summary = "查找未完成的面试会话",
            description = "根据简历 ID 查找该简历下是否存在未完成（CREATED 或 IN_PROGRESS）的面试会话，"
                    + "用于断线重连。Cache-Aside 读操作：先查 Redis，查到则返回数据。若是查不到那么回表查 MySQL ，若数据库再查不到那么抛出异常" +
                    "若是 MySQL 查到了，那么 将数据 回填到 Redis 中去")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "查找未完成的面试会话成功",
                    content = @Content(schema = @Schema(implementation = ResultInterviewSession.class))),
            @ApiResponse(responseCode = "3001", description = "面试会话不存在",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class))),
    })
    @GetMapping("/api/interview/sessions/unfinished/{resumeId}")
    public Result<InterviewSessionDTO> findUnfinishedSession(
            @Parameter(description = "简历 ID（数据库主键）", required = true, example = "1")
            @PathVariable Long resumeId) {
        return Result.success(sessionService.findUnfinishedSessionOrThrow(resumeId));
    }

    @Operation(summary = "获取面试详情",
            description = "获取指定会话的完整详情，包含所有题目、用户作答记录（未回答的题目 userAnswer "
                    + "为 null）、每题 AI 评分与反馈。用于面试历史记录查看页")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "获取面试详情成功",
                    content = @Content(schema = @Schema(implementation = ResultInterviewDetail.class))),
            @ApiResponse(responseCode = "3001", description = "面试会话不存在",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class))),
    })
    @GetMapping("/api/interview/sessions/{sessionId}/details")
    public Result<InterviewDetailDTO> getInterviewDetail(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId) {
        InterviewDetailDTO detail = historyService.getInterviewDetail(sessionId);
        return Result.success(detail);
    }

    @Operation(summary = "导出面试报告为 PDF",
            description = "将指定会话的完整评估报告导出为 PDF 文件，以附件形式触发浏览器下载。"
                    + "直接返回 PDF 字节流，不走 Result 统一响应包装")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "PDF 文件",
                    content = @Content(mediaType = MediaType.APPLICATION_PDF_VALUE,
                            schema = @Schema(type = "string", format = "byte"))),
            @ApiResponse(responseCode = "500", description = "PDF 生成失败（不走 GlobalExceptionHandler 的 HTTP 200 包装）",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class)))
    })
    @GetMapping("/api/interview/sessions/{sessionId}/export")
    public ResponseEntity<byte[]> exportInterviewPdf(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId) {
        try {
            byte[] pdfBytes = historyService.exportInterviewPdf(sessionId);
            String filename = URLEncoder.encode("模拟面试报告_" + sessionId + ".pdf",
                    StandardCharsets.UTF_8);

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename*=UTF-8''" + filename)
                    .contentType(MediaType.APPLICATION_PDF)
                    .body(pdfBytes);
        } catch (Exception e) {
            log.error("导出PDF失败", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @Operation(summary = "删除面试会话",
            description = "删除指定面试会话及其关联的所有答案数据（通过 JPA 级联自动完成）。"
                    + "使用悲观行锁防止并发删除竞态。删除后不可恢复")
    @ApiResponses({
            @ApiResponse(responseCode = "3001", description = "面试会话不存在",
                    content = @Content(schema = @Schema(implementation = ErrorResult.class))),
    })
    @DeleteMapping("/api/interview/sessions/{sessionId}")
    public Result<Void> deleteInterview(
            @Parameter(description = "面试会话唯一标识，16 位十六进制字符串",
                    required = true,
                    example = "a1b2c3d4e5f6a7b8")
            @PathVariable String sessionId) {
        log.info("删除面试会话: {}", sessionId);
        persistenceService.deleteSessionBySessionId(sessionId);
        return Result.success(null);
    }
}
