package interview.guide.infrastructure.kafka;

import interview.guide.modules.resume.listener.ResumeAnalyzeDlqHandler;
import interview.guide.modules.resume.model.ResumeAnalyzeTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 基础设施配置（当前仅为简历分析模块服务）。
 * <p>
 * - 生产者/消费者均使用 JSON 序列化 {@link ResumeAnalyzeTask}；
 * - 消费者手动 ACK（成功后才提交 offset）；
 * - 失败由 {@link DefaultErrorHandler} 按指数退避重试（最多 {@link KafkaProperties#getMaxAttempts()} 次），
 * 重试耗尽后写入死信 Topic 并回调 {@link ResumeAnalyzeDlqHandler} 将 DB 状态标记为 FAILED。
 */
@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    /**
     * 死信发送的最大等待时间（秒）
     */
    private static final long DLQ_SEND_TIMEOUT_SECONDS = 5;


    // =========================================================
    // 生产者：String key + JSON value
    // =========================================================
    @Bean
    public ProducerFactory<String, ResumeAnalyzeTask> producerFactory(KafkaProperties props) {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        // 幂等生产者：acks = all + 重试，保证不重不漏
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 单次发送的最长交付时长：Kafka 内部在该时长内自动重试，超时即判定本次发送失败，
        // 便于生产者在上层做 业务重试 / 死信补偿（否则 broker 不可用时会一直缓冲，无法感知失败）。
        // 注意：Kafka 要求该值为 32 位 int，传入 long 会在创建生产者时配置校验失败。
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) props.getSendDeliveryTimeoutMs());

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, ResumeAnalyzeTask> kafkaTemplate(
            ProducerFactory<String, ResumeAnalyzeTask> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // =========================================================
    // 消费者：String key + JSON value，手动提交 offset
    // =========================================================
    @Bean
    public ConsumerFactory<String, ResumeAnalyzeTask> consumerFactory(KafkaProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, props.getResumeAnalyzeGroup());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JacksonJsonDeserializer<ResumeAnalyzeTask> valueDeserializer =
                new JacksonJsonDeserializer<>(ResumeAnalyzeTask.class);
        // 告诉 Jackson JSON 反序列化器：来自这个 Java 包的类型可以被信任。
        valueDeserializer.addTrustedPackages("interview.guide.modules.resume.listener");

        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(), // 反序列化 key
                valueDeserializer         // 反序列化 value 为 Java 对象
        );
    }

    // =========================================================
    // 监听容器工厂：手动 ACK + 指数退避重试 + DLQ
    // =========================================================
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, ResumeAnalyzeTask>>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, ResumeAnalyzeTask> consumerFactory,
            KafkaTemplate<String, ResumeAnalyzeTask> kafkaTemplate,
            ResumeAnalyzeDlqHandler dlqHandler,
            KafkaProperties props) {

        ConcurrentKafkaListenerContainerFactory<String, ResumeAnalyzeTask> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.getConsumerCount());
        factory.setMissingTopicsFatal(false); // Topic 由生产者首次发送时自动创建
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // 开启投递次数头：消费者可从 KafkaHeaders.DELIVERY_ATTEMPT 读到"第几次尝试"，便于日志观察重试
        factory.getContainerProperties().setDeliveryAttemptHeader(true);

        // 最终失败（重试耗尽）：写入死信 Topic + 回调业务标记 FAILED。
        // 死信写入为"尽力而为"：即使写入失败也必须继续标记 FAILED 并提交 offset，
        // 否则 DefaultErrorHandler 会判定"未恢复"→ 无限 seek 重投（死循环）。
        // 这里直接用 KafkaTemplate 发送（可控超时），避开 DeadLetterPublishingRecoverer 的
        // 内部发送超时判定逻辑。
        DefaultErrorHandler errorHandler = getDefaultErrorHandler(kafkaTemplate, dlqHandler, props);
        errorHandler.setCommitRecovered(true); // 恢复（进 DLQ）后提交 offset，避免重复重投
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    static @NonNull DefaultErrorHandler getDefaultErrorHandler(KafkaTemplate<String, ResumeAnalyzeTask> kafkaTemplate, ResumeAnalyzeDlqHandler dlqHandler, KafkaProperties props) {
        // 最终失败（重试耗尽）处理：写入死信 Topic + 回调标记 FAILED，全部"尽力而为"。
        // DefaultErrorHandler 在 recoverer 正常返回后判定"已恢复"并提交 offset（setCommitRecovered(true)）；
        // 若 recoverer 抛异常会被判定为"未恢复"→ 无限 seek 重投（死循环），故此处绝不抛出。
        ConsumerRecordRecoverer recoverer = (record, ex) -> {
            if (record.value() instanceof ResumeAnalyzeTask task) {
                Long resumeId = task.resumeId();
                try {
                    kafkaTemplate.send(props.getResumeAnalyzeDlqTopic(), String.valueOf(resumeId), task)
                            .get(DLQ_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    log.warn("简历分析任务已写入死信队列: resumeId={}, topic={}",
                            resumeId, props.getResumeAnalyzeDlqTopic());
                } catch (Exception dlqEx) {
                    // 死信写入失败：仅记录日志，仍需继续标记 FAILED 并提交 offset，避免无限重投
                    log.error("写入死信队列失败（继续标记 FAILED 并提交 offset）: resumeId={}, error={}",
                            resumeId, dlqEx.getMessage(), dlqEx);
                }
                try {
                    dlqHandler.markFinalFailure(task, ex);
                } catch (Exception markEx) {
                    // 标记 FAILED 失败（如简历已删除）：同样尽力而为，不回抛
                    log.error("标记简历分析 FAILED 失败: resumeId={}, error={}",
                            resumeId, markEx.getMessage(), markEx);
                }
            }
        };

        // 指数退避：首次 1s，随后 2s、4s…
        // setMaxAttempts(N-1) 表示重试 N-1 次后停止（连同首次投递共处理 N 次，见 nextBackOff 的 attempts 计数语义）
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(props.getRetryInitialBackoffMs());
        backOff.setMultiplier(props.getRetryMultiplier());
        backOff.setMaxInterval(props.getRetryMaxBackoffMs());
        backOff.setMaxAttempts(props.getMaxAttempts() - 1);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    // =========================================================
    // KafkaAdmin + Topic：启动时预创建业务 Topic。
    // 避免"首次发送时才自动创建"导致的 LEADER_NOT_AVAILABLE 竞态
    // =========================================================
    @Bean
    public KafkaAdmin kafkaAdmin(KafkaProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        return new KafkaAdmin(config);
    }

    @Bean
    public NewTopic resumeAnalyzeTopic(KafkaProperties props) {
        return new NewTopic(props.getResumeAnalyzeTopic(), 1, (short) 1);
    }

    @Bean
    public NewTopic resumeAnalyzeDlqTopic(KafkaProperties props) {
        return new NewTopic(props.getResumeAnalyzeDlqTopic(), 1, (short) 1);
    }
}
