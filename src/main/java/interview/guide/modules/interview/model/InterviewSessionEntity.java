package interview.guide.modules.interview.model;

import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.modules.resume.model.ResumeEntity;
import jakarta.persistence.*;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 面试会话实体
 */
@Data
@Entity
@Table(name = "interview_sessions", indexes = {
        @Index(name = "idx_interview_session_resume_created", columnList = "resume_id,created_at"),
        @Index(name = "idx_interview_session_resume_status_created", columnList = "resume_id,status,created_at"),
        @Index(name = "idx_interview_session_skill_created", columnList = "skillId,createdAt")
})
public class InterviewSessionEntity {

    // Getters and Setters
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 会话ID (UUID)
    @Column(nullable = false, unique = true, length = 36)
    private String sessionId;

    // 面试主题
    @Column(length = 64)
    private String skillId = "java-backend";

    // 难度级别 (junior / mid / senior)
    @Column(length = 16)
    private String difficulty = "mid";

    // 简历ID（直接映射FK列，避免LAZY加载触发额外查询）
    @Column(name = "resume_id", insertable = false, updatable = false)
    private Long resumeId;

    // 关联的简历（可选，支持无简历通用面试）
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "resume_id")
    private ResumeEntity resume;

    // 问题总数
    private Integer totalQuestions;

    // 当前问题索引
    private Integer currentQuestionIndex = 0;

    // 会话状态
    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    private SessionStatus status = SessionStatus.CREATED;

    // 问题列表 (JSON格式)
    @Column(columnDefinition = "TEXT")
    private String questionsJson;

    // 总分 (0-100)
    private Integer overallScore;

    // 总体评价
    @Column(columnDefinition = "TEXT")
    private String overallFeedback;

    // 优势 (JSON)
    @Column(columnDefinition = "TEXT")
    private String strengthsJson;

    // 改进建议 (JSON)
    @Column(columnDefinition = "TEXT")
    private String improvementsJson;

    // 参考答案 (JSON)
    @Column(columnDefinition = "TEXT")
    private String referenceAnswersJson;

    // 面试答案记录
    @OneToMany(mappedBy = "session", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<InterviewAnswerEntity> answers = new ArrayList<>();

    // 创建时间
    @Column(nullable = false)
    private LocalDateTime createdAt;

    // 完成时间
    private LocalDateTime completedAt;

    // 评估状态（异步评估）
    @Enumerated(EnumType.STRING)
    @Column(length = 20)
    private AsyncTaskStatus evaluateStatus;

    // 评估开始时间（租约起点）
    // tryClaimForEvaluation 抢占成功时写入，回到 PENDING 时清空。
    // 补偿任务据此识别 "崩溃卡死" 的 PROCESSING 会话（超过租约阈值即重置重投）。
    private LocalDateTime evaluateStartedAt;

    // 评估错误信息
    @Column(length = 500)
    private String evaluateError;

    // LLM提供商
    @Column(length = 50)
    private String llmProvider = "dashscope";

    public enum SessionStatus {
        CREATED,      // 会话已创建
        IN_PROGRESS,  // 面试进行中
        COMPLETED,    // 面试已完成
        EVALUATED     // 已生成评估报告
    }

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
    }

    public void addAnswer(InterviewAnswerEntity answer) {
        answers.add(answer);
        answer.setSession(this);
    }
}
