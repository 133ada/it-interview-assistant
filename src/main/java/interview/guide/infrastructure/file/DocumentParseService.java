package interview.guide.infrastructure.file;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * 通用文档解析服务
 * 使用 Apache Tika 解析多种文档格式（PDF、Word、Markdown等），完美提取纯文本内容
 * 供知识库向量化（RAG）和简历大模型分析模块共同调用
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentParseService {

    private static final int MAX_TEXT_LENGTH = 5 * 1024 * 1024; // 5MB

    // 注入文本清洗服务，用于剔除 Tika 抽出来之后的连续多余换行、前后空格、或某些奇怪的不可见控制字符
    private final TextCleaningService textCleaningService;


    /**
     * 入口方法 A：解析前端直接上传的 MultipartFile 文件，提取纯文本
     *
     * @param file 浏览器上传过来的文件对象（支持 PDF、DOCX、DOC、TXT、MD 等）
     * @return 提取完毕并洗干净的纯文本字符串
     */
    public String parseContent(MultipartFile file) {
        String fileName = file.getOriginalFilename(); // 获取原始文件名
        log.info("开始解析文件: {}", fileName);

        // 安全防御：处理前端传过来的空文件，直接拦截返回空字符串
        if (file.isEmpty() || file.getSize() == 0) {
            log.warn("文件为空: {}", fileName);
            return "";
        }

        // 使用 try-with-resources 安全打开文件输入流，并在执行完后自动释放句柄
        try (InputStream inputStream = file.getInputStream()) {
            String content = parseContent(inputStream); // 调用核心私有方法，执行 Tika 深度内容抽取
            String cleanedContent = textCleaningService.cleanText(content); // 对抽出来的文本进行格式化纠偏和清洗
            log.info("文件解析成功，提取文本长度: {} 字符", cleanedContent.length());
            return cleanedContent; // 返回可直接喂给大模型的文本
        } catch (IOException | TikaException | SAXException e) {
            // 联合捕获：将一切流中断、Tika 破损、SAX 骨架异常统一打包为自定义业务异常向上抛出
            log.error("文件解析失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "文件解析失败: " + e.getMessage());
        }
    }

    /**
     * 入口方法 B：重载方法，解析已经存在于内存中的字节数组（byte[]）形式的文件内容
     * 适用于分布式流处理、或者从数据库/外部网关刚拉取下来的二进制简历快照
     *
     * @param fileBytes 文件的完整字节数组
     * @param fileName  原始文件名（仅用于打印日志便于排查问题）
     * @return 提取的文本内容
     */
    public String parseContent(byte[] fileBytes, String fileName) {
        log.info("开始解析文件（从字节数组）: {}", fileName);

        // 处理空文件
        if (fileBytes == null || fileBytes.length == 0) {
            log.warn("文件字节数组为空: {}", fileName);
            return "";
        }

        try (InputStream inputStream = new ByteArrayInputStream(fileBytes)) {
            String content = parseContent(inputStream);
            String cleanedContent = textCleaningService.cleanText(content);
            log.info("文件解析成功，提取文本长度: {} 字符", cleanedContent.length());
            return cleanedContent;
        } catch (IOException | TikaException | SAXException e) {
            log.error("文件解析失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "文件解析失败: " + e.getMessage());
        }
    }

    /**
     * 极客核心私有方法：使用显式 Parser + Context 上下文联动配置，进行底层的流式解析
     * <p>
     * 针对简历与知识库优化的四大亮点：
     * 1. 采用 BodyContentHandler 物理隔绝，只拿标签内的正文，过滤多余 XML 标签。
     * 2. 注入无操作提取器，遇到 Word 内嵌的庞大 PPT、Excel 附件或图片时直接忽略，大幅省开销。
     * 3. 针对 PDF 多栏布局进行坐标排序，防止左右分栏的简历被横向错乱读取。
     * 4. 显式相互绑定 Parser 增强内部解耦和执行稳健度。
     */
    private String parseContent(InputStream inputStream) throws IOException, TikaException, SAXException {
        // 1. 实例化自动检测解析器：它能基于流的指纹自动识别对面是 PDF 还是 Word，并自动调配对应子解析器（如 PDFParser、OfficeParser）
        AutoDetectParser parser = new AutoDetectParser();

        // 2. 创建内容处理器，只接收正文，限制最大长度为 5MB ，防止恶意超长文件把 handler 内存撑爆
        BodyContentHandler handler = new BodyContentHandler(MAX_TEXT_LENGTH);

        // 3. 创建元数据对象，用来接收文件自带的隐含属性（如作者、创建时间、标题等，这里暂不需要，但必须作为入参传入）
        Metadata metadata = new Metadata();

        // 4. 创建解析上下文
        ParseContext context = new ParseContext();

        // 5. 显式指定 Parser 到 Context（增强健壮性），将 parser 本身以类名作为 Key 注册进上下文，确保内部嵌套组件在需要级联解析时，能无缝寻址到主解析器
        context.set(Parser.class, parser);

        // 强制将 EmbeddedDocumentExtractor（嵌入式文档提取器）覆盖为自定义的空操作提取器（NoOpEmbeddedDocumentExtractor）。
        // 这样一来，当简历里嵌套了复杂的图片、矢量图或者关联附件时，Tika 只会提取纯文字，绝对不会在后台去解压并抽取那些图片资源，大大加快了解析速度，降低 CPU 消耗。
        context.set(EmbeddedDocumentExtractor.class, new NoOpEmbeddedDocumentExtractor());

        // 7. PDF 专用配置：关闭图片提取，按位置排序文本
        PDFParserConfig pdfConfig = new PDFParserConfig();
        pdfConfig.setExtractInlineImages(false); // 明确关闭 PDF 的内联图片提取功能（只解析文字，绝不抽取图片）
        pdfConfig.setSortByPosition(true); // 按 x/y 坐标排序文本，改善多栏布局解析顺序
        // 注意：Tika 2.9.2 中 setExtractAnnotations 方法可能不存在，关闭图片提取已足够
        context.set(PDFParserConfig.class, pdfConfig);

        // 8. 执行解析
        parser.parse(inputStream, handler, metadata, context);

        // 9. 返回提取的文本内容
        return handler.toString();
    }

    /**
     * 打通存储服务的闭环（从存储桶自动下载并解析）
     * 适用于异步大模型分析队列：工作线程拿到数据库的任务 ID $\rightarrow$ 驱动此方法去 MinIO 取文件 $\rightarrow$ 直接原地文本化。
     *
     * @param storageService   刚才介绍过的文件存储服务组件
     * @param storageKey       文件在 MinIO 里的全局唯一 Key
     * @param originalFilename 原始文件名
     * @return 提取完毕的文本内容
     */
    public String downloadAndParseContent(FileStorageService storageService, String storageKey, String originalFilename) {
        try {
            // 直接调用 storageService 核心的同步下载方法，将文件拽回 JVM 内存
            byte[] fileBytes = storageService.downloadFile(storageKey);
            // 鲁棒性检查：如文件损坏或者下载结果为空，抛错阻断
            if (fileBytes == null || fileBytes.length == 0) {
                throw new BusinessException(ErrorCode.INTERNAL_ERROR, "下载文件失败");
            }
            // 内部流水线无缝打通：直接将拉回来的字节数组喂给上面的“入口方法 B”完成文本提取
            return parseContent(fileBytes, originalFilename);
        } catch (BusinessException e) {
            throw e; // 如果已经是明确的自定义业务异常，直接透传往外扔
        } catch (Exception e) {
            // 捕捉意外未知的第三方框架网络超时、IO 断开等泛化异常
            log.error("下载并解析文件失败: storageKey={}, error={}", storageKey, e.getMessage(), e);
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "下载并解析文件失败: " + e.getMessage());
        }
    }
}
