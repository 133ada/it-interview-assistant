package interview.guide.modules.interview.model;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 面试答案实体
 */
@Entity
@Table(name = "interview_answers",
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_interview_answer_session_question", columnNames = {"session_id", "question_index"})
        },
        indexes = {
                @Index(name = "idx_interview_answer_session_question", columnList = "session_id,question_index")
        })
@Data
public class InterviewAnswerEntity {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 关联的会话
    // 所属面试会话表示：当前答案属于哪个面试会话，数据库中，实际保存的是，session_id 外键，而不是：Java对象本身。
    // @ManyToOne 表示含义：多对一  (fetch = FetchType.LAZY) 懒加载方式
    // @JoinColumn 外键的名称  nullable 外键非空
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "session_id", nullable = false)
    private InterviewSessionEntity session;

    // 问题索引
    @Column(name = "question_index")
    private Integer questionIndex;

    // 问题内容
    @Column(columnDefinition = "TEXT")
    private String question;

    // 问题类别
    private String category;

    // 用户答案
    @Column(columnDefinition = "TEXT")
    private String userAnswer;

    // 得分 (0-100)
    private Integer score;

    // 反馈
    @Column(columnDefinition = "TEXT")
    private String feedback;

    // 参考答案
    @Column(columnDefinition = "TEXT")
    private String referenceAnswer;

    // 关键点 (JSON)
    @Column(columnDefinition = "TEXT")
    private String keyPointsJson;

    // 回答时间
    @Column(nullable = false)
    private LocalDateTime answeredAt;

    @PrePersist
    protected void onCreate() {
        answeredAt = LocalDateTime.now();
    }

}
