package interview.guide.modules.resume.repository;

import interview.guide.modules.resume.model.ResumeMessageDedupEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 简历分析消息幂等仓库。
 */
@Repository
public interface ResumeMessageDedupRepository extends JpaRepository<ResumeMessageDedupEntity, Long> {

    /**
     * 判断消息是否已处理过（幂等快路径，避免重复调用 LLM）
     */
    boolean existsByMessageId(String messageId);

    /**
     * 删除某简历的全部幂等记录（简历删除时清理孤儿行）
     */
    void deleteByResumeId(Long resumeId);
}
