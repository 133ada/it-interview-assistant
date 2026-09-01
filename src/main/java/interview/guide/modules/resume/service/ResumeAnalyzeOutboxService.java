package interview.guide.modules.resume.service;

import interview.guide.modules.resume.model.OutboxStatus;
import interview.guide.modules.resume.model.ResumeAnalyzeOutboxEntity;
import interview.guide.modules.resume.repository.ResumeAnalyzeOutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * 简历分析事务性 Outbox 服务。
 * <p>
 * 职责：
 * <ul>
 *   <li>{@link #createTask} 在同一数据库事务内创建待投递任务（REQUIRED 传播，
 *       加入调用方事务，与简历入库/状态重置原子提交）；</li>
 *   <li>为中继 {@code ResumeAnalyzeOutboxRelay} 提供批次拉取与守卫式状态迁移助手。</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ResumeAnalyzeOutboxService {

    private final ResumeAnalyzeOutboxRepository outboxRepository;

    /**
     * 创建待投递任务。messageId 在创建时生成并持久化，中继重试期间保持稳定（消费者幂等去重依赖）。
     *
     * @return 生成的 messageId
     */
    @Transactional(rollbackFor = Exception.class)
    public String createTask(Long resumeId, String content) {
        String messageId = UUID.randomUUID().toString();
        ResumeAnalyzeOutboxEntity entity = ResumeAnalyzeOutboxEntity.builder()
                .messageId(messageId)
                .resumeId(resumeId)
                .content(content)
                .status(OutboxStatus.PENDING)
                .build();
        outboxRepository.save(entity);
        log.debug("已创建简历分析 Outbox 任务: outboxId={}, resumeId={}, messageId={}",
                entity.getId(), resumeId, messageId);
        return messageId;
    }

    /**
     * 拉取一批待投递任务（id 升序）
     */
    @Transactional(readOnly = true)
    public List<ResumeAnalyzeOutboxEntity> findPendingBatch(int limit) {
        return outboxRepository.findByStatusOrderByIdAsc(
                OutboxStatus.PENDING, PageRequest.of(0, limit));
    }

    @Transactional(rollbackFor = Exception.class)
    public boolean markSent(Long id) {
        return outboxRepository.updateStatusIf(id, OutboxStatus.SENT, null, OutboxStatus.PENDING) == 1;
    }

    @Transactional(rollbackFor = Exception.class)
    public boolean markDlq(Long id, String error) {
        return outboxRepository.updateStatusIf(id, OutboxStatus.DLQ, error, OutboxStatus.PENDING) == 1;
    }

    @Transactional(rollbackFor = Exception.class)
    public boolean markFailed(Long id, String error) {
        return outboxRepository.updateStatusIf(id, OutboxStatus.FAILED, error, OutboxStatus.PENDING) == 1;
    }

    /**
     * 重试计数 +1（仅 PENDING 生效），返回新的计数
     */
    @Transactional(rollbackFor = Exception.class)
    public int incrementRetryCount(Long id) {
        return outboxRepository.incrementRetryCount(id, OutboxStatus.PENDING);
    }

    @Transactional(rollbackFor = Exception.class)
    public void deleteRow(Long id) {
        outboxRepository.deleteById(id);
    }

    /**
     * 删除某简历的全部 Outbox 行（简历删除时清理孤儿行）
     */
    @Transactional(rollbackFor = Exception.class)
    public void deleteByResumeId(Long resumeId) {
        outboxRepository.deleteByResumeId(resumeId);
    }
}
