package interview.guide.infrastructure.file;

import interview.guide.common.config.StorageConfigProperties;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * 文件存储服务
 * 封装了对兼容 S3 协议存储（如 MinIO）的增删改查通用操作
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FileStorageService {

    private final S3Client s3Client;
    private final StorageConfigProperties storageConfig;

    /**
     * 上传简历文件
     * 自动归类到对象存储中的 "resumes" 虚拟目录下
     */
    public String uploadResume(MultipartFile file) {
        return uploadFile(file, "resumes");
    }

    /**
     * 删除简历文件
     */
    public void deleteResume(String fileKey) {
        deleteFile(fileKey);
    }

    /**
     * 上传知识库文件
     * 自动归类到对象存储中的 "knowledgebases" 虚拟目录下
     */
    public String uploadKnowledgeBase(MultipartFile file) {
        return uploadFile(file, "knowledgebases");
    }

    /**
     * 删除知识库文件
     */
    public void deleteKnowledgeBase(String fileKey) {
        deleteFile(fileKey);
    }

    /**
     * 下载文件（通用方法）
     *
     * @param fileKey 文件在对象存储中的唯一标识键（Key）
     * @return 文件的二进制字节数组
     */
    public byte[] downloadFile(String fileKey) {
        // 安全防御：首先检查文件在服务器上是否存在，不存在直接阻断
        if (!fileExists(fileKey)) {
            throw new BusinessException(ErrorCode.STORAGE_DOWNLOAD_FAILED, "文件不存在: " + fileKey);
        }

        try {
            // 构建 S3 标准获取对象请求
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(storageConfig.getBucket())
                    .key(fileKey)
                    .build();
            // 向存储服务端发起同步调用，并将返回的流数据一次性转换为内存字节数组返回
            return s3Client.getObjectAsBytes(getRequest).asByteArray();
        } catch (S3Exception e) {
            // 捕捉 S3 客户端通信或鉴权异常
            log.error("下载文件失败: {} - {}", fileKey, e.getMessage(), e);
            throw new BusinessException(ErrorCode.STORAGE_DOWNLOAD_FAILED, "文件下载失败: " + e.getMessage());
        }
    }

    // 定义文件在存储桶内部按照“年/月/日”分级离散存储的路径格式化器，防止单文件夹下文件过多产生性能瓶颈
    private static final DateTimeFormatter DATE_PATH_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    /**
     * 内部通用私有文件上传方法
     * 核心逻辑：生成唯一 Key $\rightarrow$ 打包 Request $\rightarrow$ 通过流上传
     */
    private String uploadFile(MultipartFile file, String prefix) {
        String originalFilename = file.getOriginalFilename(); // 获取前端上传文件的物理原名
        String fileKey = generateFileKey(originalFilename, prefix); // 结合业务前缀、时间线和 UUID 生成防冲突的唯一键

        try {
            // 构建 S3 的写入对象请求，传递关键元数据（避免服务端因缺少 Content-Length 而报 411 错）
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(storageConfig.getBucket())
                    .key(fileKey)
                    .contentType(file.getContentType()) // 保持文件原本的 MIME 类型（如 application/pdf）
                    .contentLength(file.getSize()) // 传入文件大小
                    .build();

            // 核心推送：将文件输入流包装为 RequestBody，流式写入到 MinIO / RustFS 中
            s3Client.putObject(putRequest, RequestBody.fromInputStream(file.getInputStream(), file.getSize()));
            log.info("文件上传成功: {} -> {}", originalFilename, fileKey);
            return fileKey; // 返回该文件在系统中的唯一钥匙，用于存入数据库
        } catch (IOException e) {
            log.error("读取上传文件失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.STORAGE_UPLOAD_FAILED, "文件读取失败");
        } catch (S3Exception e) {
            // 这里的异常日志信息可能暗示了后台底层的 MinIO 网关状况
            log.error("上传文件到RustFS失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.STORAGE_UPLOAD_FAILED, "文件存储失败: " + e.getMessage());
        }
    }

    /**
     * 检查文件是否存在
     * 核心机理：不拉取文件真实数据，只发起轻量级的 HEAD 请求，只读元数据，速度极快
     */
    public boolean fileExists(String fileKey) {
        try {
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(storageConfig.getBucket())
                    .key(fileKey)
                    .build();
            s3Client.headObject(headRequest); // 若文件存在，此方法将静默执行完毕
            return true;
        } catch (NoSuchKeyException e) {
            return false; // 如果捕获到 Key 不存在的专项异常，明确返回 false
        } catch (S3Exception e) {
            log.warn("检查文件存在性失败: {} - {}", fileKey, e.getMessage());
            return false;
        }
    }

    /**
     * 获取文件大小（字节）
     */
    public long getFileSize(String fileKey) {
        try {
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(storageConfig.getBucket())
                    .key(fileKey)
                    .build();
            return s3Client.headObject(headRequest).contentLength();
        } catch (S3Exception e) {
            log.error("获取文件大小失败: {} - {}", fileKey, e.getMessage());
            throw new BusinessException(ErrorCode.STORAGE_DOWNLOAD_FAILED, "获取文件信息失败");
        }
    }

    /**
     * 通用文件删除方法
     */
    private void deleteFile(String fileKey) {
        // 空键直接跳过
        if (fileKey == null || fileKey.isEmpty()) {
            log.debug("文件键为空，跳过删除");
            return;
        }

        // 检查文件是否存在，避免不必要的删除操作
        if (!fileExists(fileKey)) {
            log.warn("文件不存在，跳过删除: {}", fileKey);
            return;
        }

        try {
            // 构建删除请求并送达服务端
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(storageConfig.getBucket())
                    .key(fileKey)
                    .build();
            s3Client.deleteObject(deleteRequest);
            log.info("文件删除成功: {}", fileKey);
        } catch (S3Exception e) {
            log.error("删除文件失败: {} - {}", fileKey, e.getMessage(), e);
            throw new BusinessException(ErrorCode.STORAGE_DELETE_FAILED, "文件删除失败: " + e.getMessage());
        }
    }

    /**
     * 拼接并获取文件的完整公网/局域网直接访问 URL 链接
     * 形如：http://localhost:9000/mybucket/resumes/2026/07/06/abc_resume.pdf
     */
    public String getFileUrl(String fileKey) {
        return String.format("%s/%s/%s", storageConfig.getEndpoint(), storageConfig.getBucket(), fileKey);
    }

    /**
     * 确保存储桶（Bucket）在对象存储中一定存在
     * 常用在系统刚启动的监听初始化事件里，做到“无人值守、自动建桶”
     */
    public void ensureBucketExists() {
        try {
            HeadBucketRequest headRequest = HeadBucketRequest.builder()
                    .bucket(storageConfig.getBucket())
                    .build();
            s3Client.headBucket(headRequest); // 检查桶是否存在
            log.info("存储桶已存在: {}", storageConfig.getBucket());
        } catch (NoSuchBucketException e) {
            // 若检测到指定的桶缺失，立即触发自动化创建流程
            log.info("存储桶不存在，正在创建: {}", storageConfig.getBucket());
            CreateBucketRequest createRequest = CreateBucketRequest.builder()
                    .bucket(storageConfig.getBucket())
                    .build();
            s3Client.createBucket(createRequest);
            log.info("存储桶创建成功: {}", storageConfig.getBucket());
        } catch (S3Exception e) {
            log.error("检查存储桶失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 核心算法方法：构造防冲突、具有时序性、纯英文安全字符的文件 Key 路径
     * 最终生成样例: "resumes/2026/07/06/f3b2a1c0_ZhangSanJianLi.pdf"
     */
    private String generateFileKey(String originalFilename, String prefix) {
        LocalDateTime now = LocalDateTime.now();
        String datePath = now.format(DATE_PATH_FORMAT);
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        String safeName = sanitizeFilename(originalFilename);
        return String.format("%s/%s/%s_%s", prefix, datePath, uuid, safeName);
    }

    /**
     * 清理文件名，移除不安全的字符
     * <p>
     * 汉字转换为大驼峰拼音，保留字母、数字、点号、下划线和连字符，
     * 其他字符统一替换为下划线，防止 S3 存储出现问题。
     *
     * @param filename 原始文件名
     * @return 清理后的安全文件名
     */
    private String sanitizeFilename(String filename) {
        if (filename == null || filename.isEmpty()) {
            return "unknown";
        }
        return convertToPinyin(filename); // 调用汉字转拼音主引擎
    }

    /**
     * 将字符串中的汉字转换为大驼峰拼音，非汉字字符保持不变
     *
     * @param input 输入字符串
     * @return 转换后的字符串
     */
    private String convertToPinyin(String input) {
        // 设置 pinyin4j 的输出配置格式
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setCaseType(HanyuPinyinCaseType.LOWERCASE); // 转换为小写拼音字母（随后通过 capitalize 手动转大驼峰）
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE); // 抹除声调符号（如 āáǎà 统一变成 a）

        StringBuilder result = new StringBuilder();
        for (char ch : input.toCharArray()) { // 逐个字符进行扫描转换
            try {
                String[] pinyins = PinyinHelper.toHanyuPinyinStringArray(ch, format); // 试图将字符转换为汉字拼音数组（若非汉字，此方法返回 null）
                if (pinyins != null && pinyins.length > 0) {
                    // 首字母大写（大驼峰）
                    result.append(capitalize(pinyins[0]));
                } else {
                    // 非汉字字符直接保留，但特殊字符需要处理
                    result.append(sanitizeChar(ch));
                }
            } catch (BadHanyuPinyinOutputFormatCombination e) {
                // 遇到不认识的罕见怪异汉字编码时，作为异常安全退化为符号清洗
                result.append(sanitizeChar(ch));
            }
        }
        return result.toString();
    }

    /**
     * 字符白名单拦截器：只允许英文字母、数字、点号、下划线和中划线通过，其余（如空格、中英文特殊符号）统一用下划线强行替换
     * 目的：防止 S3 在进行 HTTP RESTful 请求传输时由于 URL 编码问题引发诡异的 400 Bad Request
     */
    private char sanitizeChar(char ch) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
                || (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-') {
            return ch;
        }
        return '_';
    }

    /**
     * 首字母大写
     */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
