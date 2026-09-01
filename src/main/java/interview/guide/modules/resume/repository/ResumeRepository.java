package interview.guide.modules.resume.repository;

import interview.guide.common.model.AsyncTaskStatus;
import interview.guide.modules.resume.model.ResumeEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * 简历Repository
 */
@Repository
public interface ResumeRepository extends JpaRepository<ResumeEntity, Long> {
    
    /**
     * 根据文件哈希查找简历（用于去重）
     */
    Optional<ResumeEntity> findByFileHash(String fileHash);
    
    /**
     * 检查文件哈希是否存在
     */
    boolean existsByFileHash(String fileHash);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    // PESSIMISTIC_WRITE：执行 SELECT ... FOR UPDATE。
    // 事务结束前其他事务无法对该行加写锁（阻塞等待），保证串行修改。
    @Query("SELECT s FROM ResumeEntity s WHERE s.id = :resumeId")
    Optional<ResumeEntity> findByResumeIdForUpdate(@Param("resumeId") Long resumeId);

    /**
     * 守卫式分析状态更新：仅当当前状态为 {@code expected} 时才迁移到 {@code status}（返回受影响行数）。
     * 用于避免过期的旧消息把 reanalyze 重置后的 PENDING 状态覆盖掉。
     */
    @Modifying
    @Query("""
            UPDATE ResumeEntity r
               SET r.analyzeStatus = :status, r.analyzeError = :error
             WHERE r.id = :id AND r.analyzeStatus = :expected
            """)
    int updateAnalyzeStatus(@Param("id") Long id,
                            @Param("status") AsyncTaskStatus status,
                            @Param("error") String error,
                            @Param("expected") AsyncTaskStatus expected);

}
