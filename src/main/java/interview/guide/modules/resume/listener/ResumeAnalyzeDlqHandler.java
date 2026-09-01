package interview.guide.modules.resume.listener;

import interview.guide.modules.resume.model.ResumeAnalyzeTask;
import interview.guide.modules.resume.service.ResumePersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 简历分析最终失败处理器。
 * <p>
 * 当 Kafka 消费者重试耗尽、消息进入死信队列时，由 {@code KafkaConfig} 的错误恢复器回调本组件，
 * 将数据库中的分析状态标记为 FAILED（保留失败原因供排查）。
 */
@Slf4j
@Component
public class ResumeAnalyzeDlqHandler {

    private final ResumePersistenceService resumePersistenceService;

    public ResumeAnalyzeDlqHandler(ResumePersistenceService resumePersistenceService) {
        this.resumePersistenceService = resumePersistenceService;
    }

    /**
     * 标记简历分析为最终失败
     *
     * @param task 分析任务
     * @param ex   最终失败原因
     */
    public void markFinalFailure(ResumeAnalyzeTask task, Exception ex) {
        resumePersistenceService.saveResumeEvaluateStateFAILED(task, ex);
    }


}
