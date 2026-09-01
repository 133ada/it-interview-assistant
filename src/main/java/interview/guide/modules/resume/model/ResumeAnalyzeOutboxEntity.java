package interview.guide.modules.resume.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 简历分析任务 Outbox 实体（事务性 Outbox）。
 * <p>
 * 与简历入库/状态重置在同一数据库事务内写入，解决"DB 更新成功但 Kafka 消息发送失败"的
 * 原子性问题；由 {@code ResumeAnalyzeOutboxRelay} 周期扫描 PENDING 行投递到 Kafka，
 * 投递成功（或进入死信）后再迁移状态，从而保证消息最终可达。
 */
@Data
@Builder
@Entity
@Table(name = "resume_analyze_outbox", indexes = {
        @Index(name = "idx_outbox_status_id", columnList = "status,id")
})
public class ResumeAnalyzeOutboxEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 全局唯一消息 ID（随消息体投递到 Kafka，消费者据此幂等去重）。
     * 在创建任务时生成并持久化，中继重试期间保持稳定。
     */
    @Column(name = "message_id", nullable = false, unique = true, length = 64)
    private String messageId;

    /**
     * 简历 ID
     */
    @Column(name = "resume_id", nullable = false)
    private Long resumeId;

    /**
     * 简历文本快照（消息负载）。保存消息发送时刻的内容，不受后续重分析影响。
     */
    @Column(name = "content", columnDefinition = "TEXT")
    private String content;

    /**
     * 投递状态
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    /**
     * 已尝试投递次数
     */
    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private int retryCount = 0;

    /**
     * 最近一次失败原因
     */
    @Column(name = "error", length = 500)
    private String error;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public ResumeAnalyzeOutboxEntity() {

    }

    public ResumeAnalyzeOutboxEntity(Long id, String messageId, Long resumeId, String content, OutboxStatus status, int retryCount, String error, LocalDateTime createdAt, LocalDateTime updatedAt) {
        this.id = id;
        this.messageId = messageId;
        this.resumeId = resumeId;
        this.content = content;
        this.status = status;
        this.retryCount = retryCount;
        this.error = error;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
