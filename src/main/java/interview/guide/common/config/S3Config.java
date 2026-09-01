package interview.guide.common.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

/**
 * AWS S3(Simple Storage Service)
 * S3客户端配置（用于接入自研或私有部署的 S3 兼容对象存储，如 RustFS / MinIO）
 */
@Configuration
@RequiredArgsConstructor
public class S3Config {

    private final StorageConfigProperties storageConfig;

    /**
     * 向 Spring 容器中注册一个单例的 S3Client 实例。
     */
    @Bean
    public S3Client s3Client() {
        // 创建 AWS 基础身份凭证对象，从配置类中读取并传入你的访问密钥（AccessKey）和安全密钥（SecretKey）
        AwsBasicCredentials credentials = AwsBasicCredentials.create(
            storageConfig.getAccessKey(),
            storageConfig.getSecretKey()
        );

        // 利用 S3Client 的建造者模式（Builder）开始装配和定制客户端行为
        return S3Client.builder()
                // 覆盖 AWS 默认的亚马逊云端服务器地址。将其替换为你本地或私有内网的存储服务地址（如 http://localhost:9000）
            .endpointOverride(URI.create(storageConfig.getEndpoint()))
                // 设置 S3 协议要求的区域（Region）。对于 AWS 云是必填项；对于私有化的 RustFS/MinIO 存储，可以通过配置类传入一个自定义的占位符（如 "us-east-1" 或 "rustfs"）
            .region(Region.of(storageConfig.getRegion()))
                // 将第一步创建好的身份凭证包装成一个静态凭证提供者，提供给 SDK 内部每次发起 HTTP 请求时进行签名鉴权
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
                // 6. 【关键兼容配置】：强制使用路径风格（Path-Style）访问。
                // 默认情况下（虚拟主机风格），SDK 发起请求的 URL 是 `http://bucket-name.endpoint.com/object`；
                // 开启此项后，请求的 URL 变为 `http://endpoint.com/bucket-name/object`。
                // 绝大多数私有部署存储（RustFS / MinIO）都不具备动态泛域名解析的能力，因此必须将其设为 true 才能保证正常读写。
                .forcePathStyle(true) // 关键配置：使用路径风格访问，否则 SDK 会使用虚拟主机风格（`bucket.endpoint`）导致 DNS 解析失败
            .build();
    }
}
