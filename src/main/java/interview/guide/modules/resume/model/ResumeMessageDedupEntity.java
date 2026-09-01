package interview.guide.modules.resume.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 简历分析消息幂等表。
 * <p>
 * 消费者在处理成功后，与分析结果落库处于<b>同一事务</b>写入一条 messageId 唯一记录；
 * 重复投递/重投的消息会触发唯一约束被拒，从而在 Kafka at-least-once 语义下实现幂等消费，
 * 避免同一消息重复调用 LLM 并产生重复分析记录。
 */
@Data
@Builder
@Entity
@Table(name = "resume_message_dedup", uniqueConstraints = {
        @UniqueConstraint(name = "uk_resume_msg_dedup_msg_id", columnNames = "message_id")
})
public class ResumeMessageDedupEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 全局唯一消息 ID（与 Kafka 消息体一致，幂等键）
     */
    @Column(name = "message_id", nullable = false, length = 64)
    private String messageId;

    /**
     * 简历 ID
     */
    @Column(name = "resume_id")
    private Long resumeId;

    /**
     * 处理完成时间
     */
    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    public ResumeMessageDedupEntity() {
    }

    public ResumeMessageDedupEntity(Long id, String messageId, Long resumeId, LocalDateTime processedAt) {
        this.id = id;
        this.messageId = messageId;
        this.resumeId = resumeId;
        this.processedAt = processedAt;
    }

    @PrePersist
    protected void onCreate() {
        processedAt = LocalDateTime.now();
    }
}
