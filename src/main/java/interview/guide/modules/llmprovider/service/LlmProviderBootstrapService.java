package interview.guide.modules.llmprovider.service;

import interview.guide.common.config.LlmProviderProperties;
import interview.guide.common.config.LlmProviderProperties.ProviderConfig;
import interview.guide.modules.llmprovider.model.LlmGlobalSettingEntity;
import interview.guide.modules.llmprovider.model.LlmProviderEntity;
import interview.guide.modules.llmprovider.repository.LlmGlobalSettingRepository;
import interview.guide.modules.llmprovider.repository.LlmProviderRepository;
import jakarta.annotation.PostConstruct;

import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
// 项目第一次启动时，把 application.yml 中配置好的 Provider 初始化到数据库，并初始化整个 LLM 的全局配置。
public class LlmProviderBootstrapService {

    //                   Spring Boot启动
    //                        │
    //                        ▼
    //         创建 LlmProviderBootstrapService
    //                        │
    //                        ▼
    //               @PostConstruct
    //                        │
    //                        ▼
    //          seedProvidersIfNecessary()
    //                        │
    //        ┌───────────────┴───────────────┐
    //        │                               │
    //        ▼                               ▼
    // provider表为空？                 global_setting存在？
    //        │                               │
    //      是 │                               │ 否
    //        ▼                               ▼
    //seedProviders()               ensureGlobalSetting()
    //        │                               │
    //        ▼                               ▼
    //application.yml                 初始化默认Provider
    //        │
    //        ▼
    //ProviderEntity
    //        │
    //        ▼
    //保存数据库

    private final LlmProviderProperties properties; // llm provider
    private final LlmProviderRepository providerRepository; //
    private final LlmGlobalSettingRepository globalSettingRepository;
    private final ApiKeyEncryptionService encryptionService;


    @PostConstruct
    @Transactional
    // LlmProviderBootstrapService 将 application.yml 中配置的  LlmProviderProperties 信息，保存在数据库中
    public void seedProvidersIfNecessary() {
        if (providerRepository.count() == 0) {
            seedProviders();
        }
        ensureGlobalSetting();
    }

    // 把 application.yml 中 Provider 全部导入数据库。
    private void seedProviders() {
        Map<String, ProviderConfig> providers = properties.getProviders();
        if (providers == null || providers.isEmpty()) {
            log.warn("No app.ai.providers seed configuration found");
            return;
        }

        providers.forEach((id, config) -> {
            if (isBlank(id) || config == null || isBlank(config.getBaseUrl()) || isBlank(config.getModel())) {
                log.warn("Skip invalid provider seed: id={}", id);
                return;
            }
            ApiKeyEncryptionService.EncryptedValue encrypted =
                    encryptionService.encrypt(config.getApiKey() != null ? config.getApiKey() : "");
            boolean supportsEmbedding = Boolean.TRUE.equals(config.getSupportsEmbedding())
                    || !isBlank(config.getEmbeddingModel());

            LlmProviderEntity entity = LlmProviderEntity.builder()
                    .id(id)
                    .baseUrl(config.getBaseUrl())
                    .apiKeyNonce(encrypted.nonce())
                    .apiKeyCiphertext(encrypted.ciphertext())
                    .model(config.getModel())
                    .embeddingModel(trimOrNull(config.getEmbeddingModel()))
                    .embeddingDimensions(resolveEmbeddingDimensions(config.getEmbeddingDimensions()))
                    .supportsEmbedding(supportsEmbedding)
                    .temperature(config.getTemperature())
                    .enabled(true)
                    .builtin(true)
                    .build();
            providerRepository.save(entity);
        });
        log.info("Seeded {} LLM providers from application configuration", providerRepository.count());
    }

    // 确保 llm_global_setting 一定存在，在整个系统中，只能存在一份 默认Chat Provider 、 默认Embedding Provider
    private void ensureGlobalSetting() {
        if (globalSettingRepository.existsById(LlmGlobalSettingEntity.SINGLETON_ID)) {
            return;
        }
        String defaultChatProvider = resolveExistingProvider(
                properties.getDefaultProvider(),
                providerRepository.findAll().stream().findFirst().map(LlmProviderEntity::getId).orElse("dashscope")
        );
        String configuredEmbeddingProvider = !isBlank(properties.getDefaultEmbeddingProvider())
                ? properties.getDefaultEmbeddingProvider()
                : defaultChatProvider;
        String defaultEmbeddingProvider = resolveExistingEmbeddingProvider(configuredEmbeddingProvider, defaultChatProvider);

        globalSettingRepository.save(LlmGlobalSettingEntity.builder()
                .id(LlmGlobalSettingEntity.SINGLETON_ID)
                .defaultChatProviderId(defaultChatProvider)
                .defaultEmbeddingProviderId(defaultEmbeddingProvider)
                .build());
        log.info("Initialized LLM global setting: chatProvider={}, embeddingProvider={}",
                defaultChatProvider, defaultEmbeddingProvider);
    }

    private String resolveExistingProvider(String preferredProvider, String fallbackProvider) {
        if (!isBlank(preferredProvider) && providerRepository.existsById(preferredProvider)) {
            return preferredProvider;
        }
        return fallbackProvider;
    }

    private String resolveExistingEmbeddingProvider(String preferredProvider, String fallbackProvider) {
        return providerRepository.findById(preferredProvider)
                .filter(this::canProvideEmbedding)
                .map(LlmProviderEntity::getId)
                .orElseGet(() -> providerRepository.findAll().stream()
                        .filter(this::canProvideEmbedding)
                        .findFirst()
                        .map(LlmProviderEntity::getId)
                        .orElse(fallbackProvider));
    }

    private boolean canProvideEmbedding(LlmProviderEntity provider) {
        return provider.isEnabled()
                && provider.isSupportsEmbedding()
                && !isBlank(provider.getEmbeddingModel());
    }

    private Integer resolveEmbeddingDimensions(Integer configuredDimensions) {
        if (configuredDimensions != null && configuredDimensions > 0) {
            return configuredDimensions;
        }
        return properties.getEmbeddingDimensions();
    }

    private String trimOrNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
