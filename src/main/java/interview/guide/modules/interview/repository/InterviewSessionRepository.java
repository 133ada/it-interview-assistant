package interview.guide.modules.interview.repository;

import interview.guide.common.model.AsyncTaskStatus; // 引入异步任务状态枚举（PENDING、PROCESSING等）
import interview.guide.modules.interview.model.InterviewSessionEntity; // 引入面试会话实体类（对应数据库表）
import interview.guide.modules.interview.model.InterviewSessionEntity.SessionStatus; // 引入枚举（面试状态：CREATED、IN_PROGRESS等）
import interview.guide.modules.interview.service.EvaluateCompensationJob;
import interview.guide.modules.resume.model.ResumeEntity; // 引入简历实体（外键关联）
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository; // JPA核心接口：提供CRUD能力（非常重要！！！）
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query; // 用于自定义JPQL查询
import org.springframework.data.repository.query.Param; // 参数绑定注解
import org.springframework.stereotype.Repository; // 标识这是一个Repository组件（数据访问层）

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * 面试会话Repository
 * <p>
 * 👉 作用：操作 InterviewSessionEntity（数据库表）
 * <p>
 * 👉 继承 JpaRepository 后，你自动拥有：
 * - save()        保存
 * - findById()    查询
 * - delete()      删除
 * - findAll()     查询全部
 * <p>
 * 👉 <InterviewSessionEntity, Long>
 * - 第一个：实体类
 * - 第二个：主键类型
 */
@Repository
public interface InterviewSessionRepository extends JpaRepository<InterviewSessionEntity, Long> {

    /**
     * 根据 sessionId 查询会话
     * <p>
     * 👉 JPA根据方法名自动生成SQL：
     * SELECT * FROM interview_session WHERE session_id = ?
     * <p>
     * 👉 Optional：
     * 避免空指针（推荐写法）
     */
    Optional<InterviewSessionEntity> findBySessionId(String sessionId);


    /**
     * 根据 sessionId 查询，并且“同时加载简历信息”
     * <p>
     * 👉 关键点：LEFT JOIN FETCH
     * - JOIN：关联查询
     * - FETCH：立即加载（避免懒加载问题）
     * <p>
     * 👉 JPQL（不是SQL！是面向对象的查询）
     * s 是 InterviewSessionEntity 的别名
     * <p>
     * 👉 等价SQL：
     * SELECT * FROM interview_session s
     * LEFT JOIN resume r ON s.resume_id = r.id
     */
    @Query("SELECT s FROM InterviewSessionEntity s LEFT JOIN FETCH s.resume WHERE s.sessionId = :sessionId")
    Optional<InterviewSessionEntity> findBySessionIdWithResume(
            @Param("sessionId") String sessionId // 绑定参数
    );


    /**
     * 根据 Resume 实体查找面试记录
     * <p>
     * 👉 ORDER BY createdAt DESC
     * 按创建时间倒序（最新的在前）
     * <p>
     * 👉 方法名解析：
     * findBy + Resume + OrderByCreatedAtDesc
     */
    List<InterviewSessionEntity> findByResumeOrderByCreatedAtDesc(ResumeEntity resume);


    /**
     * 根据 resumeId 查询（更常用）
     * <p>
     * 👉 等价SQL：
     * SELECT * FROM interview_session
     * WHERE resume_id = ?
     * ORDER BY created_at DESC
     */
    List<InterviewSessionEntity> findByResumeIdOrderByCreatedAtDesc(Long resumeId);


    /**
     * 查最近10条面试记录（用于“历史题去重”）
     * <p>
     * 👉 Top10 = LIMIT 10
     * <p>
     * 👉 等价SQL：
     * SELECT * FROM interview_session
     * WHERE resume_id = ?
     * ORDER BY created_at DESC
     * LIMIT 10
     */
    List<InterviewSessionEntity> findTop10ByResumeIdOrderByCreatedAtDesc(Long resumeId);


    /**
     * 查找“未完成”的面试
     * <p>
     * 👉 条件：
     * resumeId + 状态在某个集合中（IN）
     * <p>
     * 👉 findFirst：
     * 只取最新的一条
     * <p>
     * 👉 statuses 例子：
     * [CREATED, IN_PROGRESS]
     */
    Optional<InterviewSessionEntity> findFirstByResumeIdAndStatusInOrderByCreatedAtDesc(
            Long resumeId,                 // 简历ID
            List<SessionStatus> statuses   // 状态集合（IN查询）
    );


    /**
     * 根据 resumeId + 状态集合 查询
     * <p>
     * 👉 和上面区别：
     * - 上面：只取1条（最新）
     * - 这里：可能多条（但返回Optional说明业务期望只有一个）
     */
    Optional<InterviewSessionEntity> findByResumeIdAndStatusIn(
            Long resumeId,
            List<SessionStatus> statuses
    );


    /**
     * 查询所有面试会话（按时间倒序）
     * <p>
     * 👉 等价SQL：
     * SELECT * FROM interview_session
     * ORDER BY created_at DESC
     */
    List<InterviewSessionEntity> findAllByOrderByCreatedAtDesc();


    /**
     * 根据 skillId 查最近10条记录
     * <p>
     * 👉 用于“通用模式”下的历史去重
     */
    List<InterviewSessionEntity> findTop10BySkillIdOrderByCreatedAtDesc(String skillId);


    /**
     * 根据 resumeId + skillId 精确查最近10条
     * <p>
     * 👉 场景：
     * 某份简历 + 某技能 → 避免重复出题
     */
    List<InterviewSessionEntity> findTop10ByResumeIdAndSkillIdOrderByCreatedAtDesc(
            Long resumeId,
            String skillId
    );

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    // PESSIMISTIC_WRITE：执行 SELECT ... FOR UPDATE。
    // 事务结束前其他事务无法对该行加写锁（阻塞等待），保证串行修改。
    @Query("SELECT s FROM InterviewSessionEntity s WHERE s.sessionId = :sessionId")
    Optional<InterviewSessionEntity> findBySessionIdForUpdate(@Param("sessionId") String sessionId);

    /**
     * 批量删除某简历下的所有面试会话。
     * 先调 {@link InterviewAnswerRepository#deleteByResumeId} 删答案，再调此方法删会话。
     */
    @Modifying(clearAutomatically = true)
    @Query("DELETE FROM InterviewSessionEntity s WHERE s.resume.id = :resumeId")
    int deleteByResumeId(@Param("resumeId") Long resumeId);

    /**
     * 查询"评估提交（completedAt=用户交卷时间）已超过给定时间仍未完成评估"的会话。
     * 用于 {@link EvaluateCompensationJob} 扫描滞留的 PENDING 会话并重新投递。
     */
    List<InterviewSessionEntity> findByEvaluateStatusAndCompletedAtBefore(
            AsyncTaskStatus evaluateStatus,
            LocalDateTime completedAt
    );

    /**
     * 查询"评估租约已过期"的 PROCESSING 会话（执行者崩溃卡死）。
     * 用于 {@link EvaluateCompensationJob#rescueStuckProcessingEvaluations} 识别并重置重投。
     * evaluateStartedAt 为 NULL（历史残留数据）也视为过期，一并回收。
     */
    @Query("SELECT s FROM InterviewSessionEntity s " +
            "WHERE s.evaluateStatus = :status " +
            "AND (s.evaluateStartedAt IS NULL OR s.evaluateStartedAt < :leaseStart)")
    List<InterviewSessionEntity> findStaleProcessing(
            @Param("status") AsyncTaskStatus status,
            @Param("leaseStart") LocalDateTime leaseStart
    );
}