package interview.guide.infrastructure.file;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 文件哈希服务
 * 统一提供文件哈希计算功能，主要用于文件去重、秒传及内容篡改校验
 */
@Slf4j
@Service
public class FileHashService {

    private static final String HASH_ALGORITHM = "SHA-256"; // 指定业界公认安全且碰撞率极低的 SHA-256 算法作为全局哈希标准
    private static final int BUFFER_SIZE = 8192; // 设定流式读取大文件时的内存缓冲区大小为 8192 字节（即 8KB），兼顾内存消耗与磁盘 IO 性能

    /**
     * 计算 Spring 接收到的 MultipartFile 文件的 SHA-256 哈希值
     *
     * @param file 浏览器上传过来的文件对象
     * @return 64 位长度的十六进制哈希字符串
     */
    public String calculateHash(MultipartFile file) {
        try {
            // 快捷调用：直接提取文件的完整字节数组并交由重载方法处理
            // 注意：file.getBytes() 会将文件一次性全部加载到内存中，只适合处理较小的简历文件
            // file.getBytes()（MultipartFile 接口）会一次性将整个上传文件的所有二进制内容全部读取到内存中，并返回一个标准的字节数组（byte[]）
            // 简单理解就是：
            // 不管你上传的是纯文本（.txt）、简历（.pdf）、Word（.docx）还是面试录音（.mp3），在计算机底层其实都是一串由 0 和 1 组成的原始数据。
            // file.getBytes() 返回的 byte[] 数组，就是这些原始数据的最底层、没有任何修饰的原始二进制快照。
            return calculateHash(file.getBytes());
        } catch (IOException e) {
            // 防御型拦截：如果读取文件数据流失败，记录日志并抛出系统内部错误异常
            log.error("读取文件内容失败", e);
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "计算文件哈希失败");
        }
    }

    /**
     * 重载方法，直接计算已知字节数组（byte[]）的 SHA-256 哈希值
     *
     * @param data 已经在内存中准备好的字节数组
     * @return 十六进制哈希字符串
     */
    public String calculateHash(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM); // 初始化 Java 自带的摘要引擎，指定使用 "SHA-256"
            byte[] hashBytes = digest.digest(data); // 将整个字节数组塞入引擎，并执行最终的哈希计算。返回的是原始的二进制字节数组（长度为 32 字节）
            return bytesToHex(hashBytes); // 调用内部辅助方法，将难以直观阅读的二进制数组转换为标准的 16 进制字符串形式
        } catch (NoSuchAlgorithmException e) {
            log.error("哈希算法不支持: {}", HASH_ALGORITHM, e);
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "计算文件哈希失败");
        }
    }

    /**
     * 重载方法，流式计算文件哈希值（大文件专属优化方案）
     * 核心机理：采用分块（Chunk）循环读取的方式，内存中永远只占 8KB 空间，无论面对 10MB 还是 1GB 的大知识库文件，都绝对不会引发 OOM（内存溢出）
     *
     * @param inputStream 文件的输入流（比如直接来自 MinIO/RustFS 或大文件分片）
     * @return 十六进制哈希字符串
     */
    public String calculateHash(InputStream inputStream) {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
            return bytesToHex(digest.digest());
        } catch (NoSuchAlgorithmException | IOException e) {
            log.error("计算文件哈希失败", e);
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "计算文件哈希失败");
        }
    }

    /**
     * 内部辅助方法：将 32 位的二进制字节数组（byte[]）平滑转换为 64 位的十六进制 ASCII 字符串
     * 原因：二进制字节包含了大量不可见字符和乱码，无法直接存入 MySQL 数据库或展示在网络中
     */
    private String bytesToHex(byte[] bytes) {
        // 初始化 StringBuilder，一个 byte 对应 2 个十六进制字符，提前指定精确容量避免数组频繁扩容带来的性能损耗
        StringBuilder result = new StringBuilder(bytes.length * 2);
        // 遍历每一个字节
        for (byte b : bytes) {
            // 核心格式化解释："%02x"：x 表示转换为十六进制；2 表示保证输出 2 位宽度；0 表示不足 2 位时（如 0x05）在前面自动补 0（变成 "05"）
            result.append(String.format("%02x", b));
        }
        return result.toString(); // 返回拼装完毕的、纯小写字母加数字构成的标准哈希字符串
    }
}
