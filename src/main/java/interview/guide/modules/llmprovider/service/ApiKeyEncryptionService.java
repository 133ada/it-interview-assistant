package interview.guide.modules.llmprovider.service;

import interview.guide.common.config.LlmProviderProperties;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import jakarta.annotation.PostConstruct;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

// 写数据库之前先加密（Encrypt）
// 读取数据库之后再解密（Decrypt）
@Slf4j
@Service
public class ApiKeyEncryptionService {

    private static final int NONCE_BYTES = 12; // AES-GCM要求随机IV（Nonce）
    private static final int GCM_TAG_BITS = 128; // 认证Tag长度 （用于检测：数据有没有被修改）
    private static final String CIPHER = "AES/GCM/NoPadding"; //AES算法  模式：GCM  Padding：NoPadding
    private static final String DEV_FALLBACK_KEY =
            "interview-guide-dev-only-provider-api-key-encryption"; //开发环境默认密钥

    private final LlmProviderProperties properties;
    private final SecureRandom secureRandom = new SecureRandom(); //这是Java安全随机数。专门用于：生成Nonce
    private SecretKeySpec secretKey; // 真正AES密钥。

    public ApiKeyEncryptionService(LlmProviderProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    void init() {
        LlmProviderProperties.SecurityConfig security = properties.getSecurity();
        String configuredKey = security != null ? security.getApiKeyEncryptionKey() : null;
        if (configuredKey == null || configuredKey.isBlank()) {
            if (security != null && security.isRequireEncryptionKey()) {
                throw new BusinessException(ErrorCode.PROVIDER_CONFIG_READ_FAILED,
                        "APP_AI_CONFIG_ENCRYPTION_KEY 未配置，无法解密 Provider API Key");
            }
            log.warn("APP_AI_CONFIG_ENCRYPTION_KEY is not configured; using development fallback key");
            configuredKey = DEV_FALLBACK_KEY;
        }
        secretKey = new SecretKeySpec(resolveKeyBytes(configuredKey), "AES");
    }

    // 加密算法
    public EncryptedValue encrypt(String plainText) {
        try {
            byte[] nonce = new byte[NONCE_BYTES]; // 生成Nonce
            secureRandom.nextBytes(nonce); // 填满随机数

            Cipher cipher = Cipher.getInstance(CIPHER); // 创建Cipher
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, nonce)); // 初始化
            byte[] ciphertext = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8)); // 加密操作

            // byte[]
            //   ↓
            // Base64
            // 因为数据库不能保存byte[]
            return new EncryptedValue(
                    Base64.getEncoder().encodeToString(nonce),
                    Base64.getEncoder().encodeToString(ciphertext)
            );
        } catch (Exception e) {
            throw new BusinessException(ErrorCode.PROVIDER_CONFIG_WRITE_FAILED,
                    "加密 Provider API Key 失败", e);
        }
    }

    // 解密算法
    public String decrypt(String nonceBase64, String ciphertextBase64) {
        try {
            byte[] nonce = Base64.getDecoder().decode(nonceBase64);
            byte[] ciphertext = Base64.getDecoder().decode(ciphertextBase64);

            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, nonce));
            byte[] plainText = cipher.doFinal(ciphertext);
            return new String(plainText, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new BusinessException(ErrorCode.PROVIDER_CONFIG_READ_FAILED,
                    "解密 Provider API Key 失败，请检查 APP_AI_CONFIG_ENCRYPTION_KEY", e);
        }
    }

    private byte[] resolveKeyBytes(String configuredKey) {
        String trimmed = configuredKey.trim();
        try {
            byte[] decoded = Base64.getDecoder().decode(trimmed);
            if (decoded.length == 32) {
                return decoded;
            }
        } catch (IllegalArgumentException ignored) {
            // Fall through to SHA-256 derivation for human-readable keys.
        }
        return sha256(trimmed);
    }

    private byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new BusinessException(ErrorCode.PROVIDER_CONFIG_READ_FAILED,
                    "初始化 Provider API Key 加密密钥失败", e);
        }
    }

    public record EncryptedValue(String nonce, String ciphertext) {
    }
}
