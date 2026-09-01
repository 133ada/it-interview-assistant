package interview.guide.modules.interview.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InterviewDetailDTOTest {

    private InterviewDetailDTO dto;
    private InterviewDetailDTO.AnswerDetailDTO answerDTO;
    private LocalDateTime now;

    @BeforeEach
    void setUp() {
        now = LocalDateTime.now();

        // ── 先构造内部 Record AnswerDetailDTO ──────────────────
        answerDTO = new InterviewDetailDTO.AnswerDetailDTO(
                0,                          // questionIndex
                "请介绍你的项目经历",          // question
                "项目经验",                   // category
                "我负责了XX模块的开发",        // userAnswer
                85,                         // score
                "回答较为全面",               // feedback
                "应从背景、难点、解决方案阐述", // referenceAnswer
                List.of("清晰表达", "数据量化"),// keyPoints
                now                         // answeredAt
        );

        // ── 再构造外部 Record InterviewDetailDTO ──────────────
        dto = new InterviewDetailDTO(
                1L,                         // id
                "abc-123-def-456",          // sessionId
                5,                          // totalQuestions
                "COMPLETED",                // status
                "COMPLETED",                // evaluateStatus
                null,                       // evaluateError（正常完成时为 null）
                88,                         // overallScore
                "整体表现良好",               // overallFeedback
                now,                        // createdAt
                now,                        // completedAt
                List.of(),                  // questions（简化）
                List.of("表达清晰", "思路有条理"),    // strengths
                List.of("缺乏数据支撑"),      // improvements
                List.of(),                  // referenceAnswers（简化）
                List.of(answerDTO)          // answers
        );
    }

    // ── 测试外部 Record 的访问器 ──────────────────────────────

    @Test
    void id() {
        assertEquals(1L, dto.id());
    }

    @Test
    void sessionId() {
        assertEquals("abc-123-def-456", dto.sessionId());
    }

    @Test
    void totalQuestions() {
        assertEquals(5, dto.totalQuestions());
    }

    @Test
    void status() {
        assertEquals("COMPLETED", dto.status());
    }

    @Test
    void evaluateStatus() {
        assertEquals("COMPLETED", dto.evaluateStatus());
    }

    @Test
    void evaluateError() {
        // 正常完成时 evaluateError 应为 null
        assertNull(dto.evaluateError());
    }

    @Test
    void overallScore() {
        assertEquals(88, dto.overallScore());
    }

    @Test
    void overallFeedback() {
        assertEquals("整体表现良好", dto.overallFeedback());
    }

    @Test
    void createdAt() {
        assertEquals(now, dto.createdAt());
    }

    @Test
    void strengths() {
        assertEquals(2, dto.strengths().size());
        assertEquals("表达清晰", dto.strengths().get(0));
    }

    @Test
    void improvements() {
        assertEquals(1, dto.improvements().size());
        assertEquals("缺乏数据支撑", dto.improvements().get(0));
    }

    // ── 测试嵌套 Record 的访问器 ─────────────────────────────

    @Test
    void answers() {
        // 外层：通过 dto.answers() 拿到答案列表
        List<InterviewDetailDTO.AnswerDetailDTO> answers = dto.answers();
        assertEquals(1, answers.size());

        // 内层：拿到第一条答案，验证字段
        InterviewDetailDTO.AnswerDetailDTO first = answers.get(0);
        assertEquals(0,          first.questionIndex());
        assertEquals("请介绍你的项目经历", first.question());
        assertEquals("项目经验",  first.category());
        assertEquals("我负责了XX模块的开发", first.userAnswer());
        assertEquals(85,         first.score());
        assertEquals("回答较为全面", first.feedback());
        assertEquals(2,          first.keyPoints().size());
        assertEquals(now,        first.answeredAt());
    }

    // ── 测试自动生成的 equals ────────────────────────────────

    @Test
    void testEquals() {
        // 字段完全相同 → equals 返回 true
        InterviewDetailDTO same = new InterviewDetailDTO(
                1L, "abc-123-def-456", 5, "COMPLETED", "COMPLETED",
                null, 88, "整体表现良好", now, now,
                List.of(), List.of("表达清晰", "思路有条理"),
                List.of("缺乏数据支撑"), List.of(), List.of(answerDTO)
        );
        assertEquals(dto, same);
    }

    @Test
    void testNotEquals() {
        // 只改了 sessionId，其余相同 → 不相等
        InterviewDetailDTO different = new InterviewDetailDTO(
                1L, "zzz-999",  5, "COMPLETED", "COMPLETED",
                null, 88, "整体表现良好", now, now,
                List.of(), List.of("表达清晰", "思路有条理"),
                List.of("缺乏数据支撑"), List.of(), List.of(answerDTO)
        );
        assertNotEquals(dto, different);
    }

    // ── 测试自动生成的 toString ──────────────────────────────

    @Test
    void testToString() {
        String str = dto.toString();
        System.out.println(str);
        // 输出格式：InterviewDetailDTO[id=1, sessionId=abc-123-def-456, ...]
        assertTrue(str.contains("InterviewDetailDTO"));
        assertTrue(str.contains("abc-123-def-456"));
        assertTrue(str.contains("88"));
    }

    // ── 测试不可变性 ─────────────────────────────────────────

    @Test
    void testImmutable() {
        // Record 没有 setter，所有字段 final
        // dto.setStatus("FAILED");  // ❌ 编译直接报错

        // 想"修改"只能新建对象，原对象不受影响
        InterviewDetailDTO updated = new InterviewDetailDTO(
                dto.id(), dto.sessionId(), dto.totalQuestions(),
                "FAILED",           // 只改这一个字段
                dto.evaluateStatus(), dto.evaluateError(),
                dto.overallScore(), dto.overallFeedback(),
                dto.createdAt(), dto.completedAt(),
                dto.questions(), dto.strengths(),
                dto.improvements(), dto.referenceAnswers(),
                dto.answers()
        );

        assertEquals("FAILED",    updated.status()); // 新对象已改
        assertEquals("COMPLETED", dto.status());     // 原对象不变 ✅
    }
}