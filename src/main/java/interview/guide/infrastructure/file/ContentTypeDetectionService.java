package interview.guide.infrastructure.file;

import lombok.extern.slf4j.Slf4j;
import org.apache.tika.Tika;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;

/**
 * 文件内容类型检测服务
 * 使用 Apache Tika 进行精确的 MIME 类型检测，防止伪造后缀名的文件上传攻击
 */
@Slf4j
@Service
    public class ContentTypeDetectionService {

    private final Tika tika; // 声明 Tika 探测器实例

    public ContentTypeDetectionService() {
        this.tika = new Tika();
    }

    /**
     * 检测 Spring 接收到的 MultipartFile 文件的真实 MIME 类型
     * 通过嗅探文件前几个字节的“魔数（Magic Number）”进行判断，比直接读取浏览器传入的 HTTP 头部安全且准确得多
     *
     * @param file 浏览器上传过来的 MultipartFile 文件对象
     * @return MIME 类型字符串（如 "application/pdf"、"image/png"）
     */
    public String detectContentType(MultipartFile file) {
        // 使用 try-with-resources 语法，确保读取完毕后自动关闭文件的输入流（InputStream），防止内存/句柄泄漏
        try (InputStream inputStream = file.getInputStream()) {
            // Tika 会读取流中的头部指纹，同时结合文件名后缀进行联合精准判定
            return tika.detect(inputStream, file.getOriginalFilename());
        } catch (IOException e) {
            log.warn("无法检测文件类型，使用 Content-Type 头部: {}", e.getMessage());
            return file.getContentType();
        }
    }

    /**
     * 重载方法，直接检测底层 Java 输入流（InputStream）的 MIME 类型
     * 适用于检测从 MinIO/RustFS 对象存储下载的文件流，或是网络下载的音视频切片流
     *
     * @param inputStream 需要被探测的物理输入流
     * @param fileName    原始文件名（可选，用于在内容模糊时辅助 Tika 猜测类型）
     * @return MIME 类型字符串
     */
    public String detectContentType(InputStream inputStream, String fileName) {
        try {
            return tika.detect(inputStream, fileName);
        } catch (IOException e) {
            log.warn("无法检测文件类型: {}", e.getMessage());
            return "application/octet-stream";
        }
    }

    /**
     * 重载方法，直接探测已经在内存中展开的字节数组（byte[]）的 MIME 类型
     * 适用于检测直接通过网络下载好的完整简历数据包，或者直接在内存中生成的 PDF 字节流
     *
     * @param data     文件的完整或头部字节数组
     * @param fileName 原始文件名（可选）
     * @return MIME 类型字符串
     */
    public String detectContentType(byte[] data, String fileName) {
        return tika.detect(data, fileName);
    }

    /**
     * 判断是否为 PDF 文件
     */
    public boolean isPdf(String contentType) {
        return contentType != null && contentType.toLowerCase().contains("pdf");
    }

    /**
     * 判断是否为 Word 文档（DOC/DOCX）
     */
    public boolean isWordDocument(String contentType) {
        if (contentType == null) return false;
        String lower = contentType.toLowerCase();
        // 解释："msword" 对应老版 Word（.doc）；"wordprocessingml" 对应基于 OpenXML 的新版 Word（.docx）
        return lower.contains("msword") || lower.contains("wordprocessingml");
    }

    /**
     * 判断是否为纯文本文件
     */
    public boolean isPlainText(String contentType) {
        return contentType != null && contentType.toLowerCase().startsWith("text/");
    }

    /**
     * 判断是否为 Markdown 文件
     */
    public boolean isMarkdown(String contentType, String fileName) {
        // 策略一：优先通过探测出来的 MIME 内容进行判断
        if (contentType != null) {
            String lower = contentType.toLowerCase();
            // 部分小众或自定义网关会将 Markdown 识别为 "text/markdown" 或 "text/x-markdown"
            if (lower.contains("markdown") || lower.contains("x-markdown")) {
                return true;
            }
        }
        // 策略二：内容判断未命中时，降级采用后缀名规则进行严格审查
        if (fileName != null) {
            String lowerName = fileName.toLowerCase();
            return lowerName.endsWith(".md") ||
                    lowerName.endsWith(".markdown") ||
                    lowerName.endsWith(".mdown");
        }
        return false;
    }
}
