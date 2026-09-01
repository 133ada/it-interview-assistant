package interview.guide.modules.resume.repository;

import interview.guide.modules.resume.model.OutboxStatus;
import interview.guide.modules.resume.model.ResumeAnalyzeOutboxEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 简历分析 Outbox 仓库。
 */
@Repository
public interface ResumeAnalyzeOutboxRepository extends JpaRepository<ResumeAnalyzeOutboxEntity, Long> {

    /**
     * 按状态取待投递任务（id 升序，保证投递顺序与创建顺序一致）
     */
    List<ResumeAnalyzeOutboxEntity> findByStatusOrderByIdAsc(OutboxStatus status, Pageable pageable);

    /**
     * 守卫式状态迁移：仅当当前状态为 {@code expected} 时才迁移到 {@code status}（返回受影响行数）。
     * 防止多实例/多轮中继互相覆盖状态。
     */
    @Modifying
    @Query("""
            UPDATE ResumeAnalyzeOutboxEntity o
               SET o.status = :status, o.error = :error, o.updatedAt = CURRENT_TIMESTAMP
             WHERE o.id = :id AND o.status = :expected
            """)
    int updateStatusIf(@Param("id") Long id,
                       @Param("status") OutboxStatus status,
                       @Param("error") String error,
                       @Param("expected") OutboxStatus expected);

    /**
     * 指定状态的重试计数 +1（返回新的计数）。调用方需保证状态为 PENDING。
     */
    @Modifying
    @Query("""
            UPDATE ResumeAnalyzeOutboxEntity o
               SET o.retryCount = o.retryCount + 1, o.updatedAt = CURRENT_TIMESTAMP
             WHERE o.id = :id AND o.status = :status
            """)
    int incrementRetryCount(@Param("id") Long id, @Param("status") OutboxStatus status);

    /**
     * 删除某简历的全部 Outbox 行（简历删除时清理孤儿行）
     */
    void deleteByResumeId(Long resumeId);
}
