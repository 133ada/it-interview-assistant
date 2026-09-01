package interview.guide.common.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Redis Stream 异步消费者并发配置。
 * <p>
 * 每个消费者对应一个 Redis Stream 消费者组；
 * 这里的值 = 该组内并行拉取的消费者线程数（默认 1 = 单消费者顺序消费）。
 * <p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.async.consumers")
public class AsyncConsumerProperties {

    /**
     * 知识库向量化消费者组内并行数
     */
    private int vectorize = 1;

    /**
     * 简历分析消费者组内并行数
     */
    private int analyze = 1;

    /**
     * 面试评估消费者组内并行数
     */
    private int evaluate = 1;

    /**
     * 语音面试评估消费者组内并行数
     */
    private int voiceEvaluate = 1;
}
