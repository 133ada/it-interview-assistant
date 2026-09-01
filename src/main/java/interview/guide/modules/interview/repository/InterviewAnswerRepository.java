package interview.guide.modules.interview.repository;

import interview.guide.modules.interview.model.InterviewAnswerEntity;
import interview.guide.modules.interview.model.InterviewSessionEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 面试答案Repository
 */
@Repository
public interface InterviewAnswerRepository extends JpaRepository<InterviewAnswerEntity, Long> {
    
    /**
     * 根据会话查找所有答案
     */
    List<InterviewAnswerEntity> findBySessionOrderByQuestionIndex(InterviewSessionEntity session);
    
    /**
     * 根据会话ID查找所有答案
     */
    List<InterviewAnswerEntity> findBySessionIdOrderByQuestionIndex(Long sessionId);
    
    /**
     * 根据会话 sessionId 字符串查找所有答案
     */
    List<InterviewAnswerEntity> findBySession_SessionIdOrderByQuestionIndex(String sessionId);

    /**
     * 根据会话 sessionId 和问题索引查找单条答案（用于 upsert）
     */
    Optional<InterviewAnswerEntity> findBySession_SessionIdAndQuestionIndex(String sessionId, Integer questionIndex);

    /**
     * 批量删除某简历下所有面试的答案（配合 session 批量删除，避免 N+1）。
     * JPQL DELETE 直接执行 SQL，绕过 JPA 逐条加载-删除的循环。
     */
    @Modifying(clearAutomatically = true)
    @Query("DELETE FROM InterviewAnswerEntity a WHERE a.session.resume.id = :resumeId")
    int deleteByResumeId(@Param("resumeId") Long resumeId);
}
