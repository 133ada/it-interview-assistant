package interview.guide.infrastructure.export;

import com.itextpdf.io.font.PdfEncodings;
import com.itextpdf.kernel.colors.DeviceRgb;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.font.PdfFontFactory.EmbeddingStrategy;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import com.itextpdf.layout.properties.TextAlignment;
import com.itextpdf.layout.properties.UnitValue;
import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import interview.guide.modules.interview.model.InterviewAnswerEntity;
import interview.guide.modules.interview.model.InterviewSessionEntity;
import interview.guide.modules.interview.model.ResumeAnalysisResponse;
import interview.guide.modules.resume.model.ResumeEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * PDF导出服务
 * 用于将简历分析报告与模拟面试报告导出为 PDF 文件
 * iText 是 Java 生态中最著名的 PDF 操作库之一，它可以：创建 PDF、修改 PDF、读取 PDF、合并 PDF、添加图片、添加表格、添加字体、添加签名、添加水印
 *  iText 是一个用 Java（以及 .NET）编写 PDF 文档的专业库。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PdfExportService {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"); // 定义全局的时间格式化器：年-月-日 时:分:秒
    private static final DeviceRgb HEADER_COLOR = new DeviceRgb(41, 128, 185); // 定义 PDF 报告的大标题颜色：古典蓝 (RGB: 41, 128, 185)
    private static final DeviceRgb SECTION_COLOR = new DeviceRgb(52, 73, 94); //定义 PDF 报告的二级小节标题颜色：沥青深灰 (RGB: 52, 73, 94)

    private final ObjectMapper objectMapper; // 注入 Jackson 核心的 ObjectMapper，用于将持久化在数据库中的 JSON 字符串反序列化为 Java 对象

    /**
     * 核心辅助方法：创建并加载支持中文展示的嵌入式 PDF 字体。
     * iText 7 默认只支持纯英文、西文字体，如果不进行中文字体强嵌入，PDF 中所有的汉字都会变成空白或者方块（乱码）。
     */
    private PdfFont createChineseFont() {
        // 使用 try-with-resources 语法从类路径（resources）下安全读取“朱雀仿宋”开源字体流
        try (var fontStream = getClass().getClassLoader().getResourceAsStream("fonts/ZhuqueFangsong-Regular.ttf")) {
            if (fontStream != null) {
                //  将字体输入流转换为字节数组
                byte[] fontBytes = fontStream.readAllBytes();
                log.debug("使用项目内嵌字体: fonts/ZhuqueFangsong-Regular.ttf");
                // 核心构建：传入字体字节、指定 IDENTITY_H (水平横排中文编码) 并强制将字体完整嵌入（FORCE_EMBEDDED）到 PDF 文件内部，确保任何人在任何设备打开都不会缺字
                return PdfFontFactory.createFont(fontBytes, PdfEncodings.IDENTITY_H, EmbeddingStrategy.FORCE_EMBEDDED);
            }

            log.error("未找到字体文件: fonts/ZhuqueFangsong-Regular.ttf");
            throw new BusinessException(ErrorCode.EXPORT_PDF_FAILED, "字体文件缺失，请联系管理员");

        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            log.error("创建中文字体失败: {}", e.getMessage(), e);
            throw new BusinessException(ErrorCode.EXPORT_PDF_FAILED, "创建字体失败: " + e.getMessage());
        }
    }

    /**
     * 核心清洗方法：清理文本中可能导致 iText 渲染崩溃或乱码的特殊字符。
     * 比如用户简历里、或者 AI 的 Feedback 中带有 Emoji（表情符号），iText 遇到无法识别的 Unicode 字符时会直接中断或崩溃。
     */
    private String sanitizeText(String text) {
        if (text == null) return "";

        // 正则表达式解释：\\p{So} 代表各种符号(含Emoji)，\\p{Cs} 代表成对的代理项（通常也是特殊字符字符集）。将其全部替换为空字符串
        return text.replaceAll("[\\p{So}\\p{Cs}]", "").trim();
    }

    /**
     * 核心业务方法 A：导出简历分析报告为 PDF 字节流
     *
     * @param resume   简历元数据
     * @param analysis 大模型生成的简历多维度分析结构体
     * @return PDF 二进制数组
     */
    public byte[] exportResumeAnalysis(ResumeEntity resume, ResumeAnalysisResponse analysis) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(); // 创建内存字节输出流，PDF 的所有生成数据最终会源源不断写入这里
        PdfWriter writer = new PdfWriter(baos); // 初始化底层低级 PDF 写入器
        PdfDocument pdfDoc = new PdfDocument(writer); // 构筑 PDF 文档结构对象
        Document document = new Document(pdfDoc); // 创建高级版面布局文档对象（所有的段落、表格都往这里加）

        // 载入并应用支持中文的字体，一劳永逸作用于整个文档
        PdfFont font = createChineseFont();
        document.setFont(font);

        // 渲染大标题：设置字号 24、加粗、居中、应用古典蓝色
        Paragraph title = new Paragraph("简历分析报告")
                .setFontSize(24)
                .setBold()
                .setTextAlignment(TextAlignment.CENTER)
                .setFontColor(HEADER_COLOR);
        document.add(title);

        // 基本信息
        document.add(new Paragraph("\n")); // 换行，撑开间距
        document.add(createSectionTitle("基本信息")); // 添加带格式的二级标题
        document.add(new Paragraph("文件名: " + resume.getOriginalFilename())); // 添加原始文件名
        document.add(new Paragraph("上传时间: " +
                (resume.getUploadedAt() != null ? DATE_FORMAT.format(resume.getUploadedAt()) : "未知"))); // 格式化并添加上传时间

        // 总分
        document.add(new Paragraph("\n"));
        document.add(createSectionTitle("综合评分"));
        // 根据最终得分（如 85 分），调用 getScoreColor 动态匹配绿色/黄色/红色，实现视觉高亮
        Paragraph scoreP = new Paragraph("总分: " + analysis.overallScore() + " / 100")
                .setFontSize(18)
                .setBold()
                .setFontColor(getScoreColor(analysis.overallScore()));
        document.add(scoreP);

        // 建立“各维度评分”表格
        if (analysis.scoreDetail() != null) {
            document.add(new Paragraph("\n"));
            document.add(createSectionTitle("各维度评分"));

            // 创建一个 2 列的表格，第一列（维度名）与第二列（得分）的宽度比例为 2:1
            Table scoreTable = new Table(UnitValue.createPercentArray(new float[]{2, 1}))
                    .useAllAvailableWidth();
            // 调用辅助函数，逐行添加各维度的子得分，并传入各自维度的满分权重（40分、20分等）
            addScoreRow(scoreTable, "项目经验", analysis.scoreDetail().projectScore(), 40);
            addScoreRow(scoreTable, "技能匹配度", analysis.scoreDetail().skillMatchScore(), 20);
            addScoreRow(scoreTable, "内容完整性", analysis.scoreDetail().contentScore(), 15);
            addScoreRow(scoreTable, "结构清晰度", analysis.scoreDetail().structureScore(), 15);
            addScoreRow(scoreTable, "表达专业性", analysis.scoreDetail().expressionScore(), 10);
            document.add(scoreTable); // 将表格渲染进 PDF
        }

        // 简历摘要
        if (analysis.summary() != null) {
            document.add(new Paragraph("\n"));
            document.add(createSectionTitle("简历摘要"));
            document.add(new Paragraph(sanitizeText(analysis.summary()))); // // 清洗文本并添加
        }

        // 优势亮点
        if (analysis.strengths() != null && !analysis.strengths().isEmpty()) {
            document.add(new Paragraph("\n"));
            document.add(createSectionTitle("优势亮点"));
            for (String strength : analysis.strengths()) {
                document.add(new Paragraph("• " + sanitizeText(strength)));
            }
        }

        // 改进建议
        if (analysis.suggestions() != null && !analysis.suggestions().isEmpty()) {
            document.add(new Paragraph("\n"));
            document.add(createSectionTitle("改进建议"));
            for (ResumeAnalysisResponse.Suggestion suggestion : analysis.suggestions()) {
                document.add(new Paragraph("【" + suggestion.priority() + "】" + sanitizeText(suggestion.category()))
                        .setBold());
                document.add(new Paragraph("问题: " + sanitizeText(suggestion.issue())));
                document.add(new Paragraph("建议: " + sanitizeText(suggestion.recommendation())));
                document.add(new Paragraph("\n"));
            }
        }

        document.close(); // 必须闭合文档，此时数据才会真正从内存刷入到流中
        return baos.toByteArray(); // 返回生成的 PDF 完整字节数组
    }

    /**
     * 核心业务方法 B：导出模拟面试报告为 PDF 字节流
     * @param session 面试会话记录实体，包含了所有的核心面试统计与级联的问答详情列表
     * @return PDF 二进制数组
     */
    public byte[] exportInterviewReport(InterviewSessionEntity session) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(); // 创建内存字节输出流，PDF 的所有生成数据最终会源源不断写入这里
        PdfWriter writer = new PdfWriter(baos); // 初始化底层低级 PDF 写入器
        PdfDocument pdfDoc = new PdfDocument(writer); // 构筑 PDF 文档结构对象
        Document document = new Document(pdfDoc); // 创建高级版面布局文档对象（所有的段落、表格都往这里加）

        // 使用支持中文的字体
        PdfFont font = createChineseFont();
        document.setFont(font);

        // 标题
        Paragraph title = new Paragraph("模拟面试报告")
                .setFontSize(24)
                .setBold()
                .setTextAlignment(TextAlignment.CENTER)
                .setFontColor(HEADER_COLOR);
        document.add(title);

        // 基本信息
        document.add(new Paragraph("\n"));
        document.add(createSectionTitle("面试信息"));
        document.add(new Paragraph("会话ID: " + session.getSessionId()));
        document.add(new Paragraph("题目数量: " + session.getTotalQuestions()));
        document.add(new Paragraph("面试状态: " + getStatusText(session.getStatus())));
        document.add(new Paragraph("开始时间: " +
                (session.getCreatedAt() != null ? DATE_FORMAT.format(session.getCreatedAt()) : "未知")));
        if (session.getCompletedAt() != null) {
            document.add(new Paragraph("完成时间: " + DATE_FORMAT.format(session.getCompletedAt())));
        }

        // 总分
        if (session.getOverallScore() != null) {
            document.add(new Paragraph("\n"));
            document.add(createSectionTitle("综合评分"));
            Paragraph scoreP = new Paragraph("总分: " + session.getOverallScore() + " / 100")
                    .setFontSize(18)
                    .setBold()
                    .setFontColor(getScoreColor(session.getOverallScore()));
            document.add(scoreP);
        }

        // 总体评价
        if (session.getOverallFeedback() != null) {
            document.add(new Paragraph("\n"));
            document.add(createSectionTitle("总体评价"));
            document.add(new Paragraph(sanitizeText(session.getOverallFeedback())));
        }

        // 优势
        if (session.getStrengthsJson() != null) {
            try {
                // 使用 Jackson 将如 '["表达流利","技术扎实"]' 还原为 List<String>
                // 因为 TypeReference 的实现会在构造时，通过反射读取这个匿名子类的泛型信息，从而知道真正的目标类型（例如 List<String>）。
                List<String> strengths = objectMapper.readValue(session.getStrengthsJson(),
                        new TypeReference<>() {
                        });
                if (!strengths.isEmpty()) {
                    document.add(new Paragraph("\n"));
                    document.add(createSectionTitle("表现优势"));
                    for (String s : strengths) {
                        document.add(new Paragraph("• " + sanitizeText(s)));
                    }
                }
            } catch (Exception e) {
                log.error("解析优势JSON失败: sessionId={}", session.getSessionId(), e);
            }
        }

        // 改进建议
        if (session.getImprovementsJson() != null) {
            try {
                List<String> improvements = objectMapper.readValue(session.getImprovementsJson(),
                        new TypeReference<>() {
                        });
                if (!improvements.isEmpty()) {
                    document.add(new Paragraph("\n"));
                    document.add(createSectionTitle("改进建议"));
                    for (String s : improvements) {
                        document.add(new Paragraph("• " + sanitizeText(s)));
                    }
                }
            } catch (Exception e) {
                log.error("解析改进建议JSON失败: sessionId={}", session.getSessionId(), e);
            }
        }

        // 问答详情
        List<InterviewAnswerEntity> answers = session.getAnswers();
        if (answers != null && !answers.isEmpty()) {
            document.add(new Paragraph("\n"));
            document.add(createSectionTitle("问答详情"));

            for (InterviewAnswerEntity answer : answers) {
                // 打印小标题：如 “问题 1 [Java并发]”
                document.add(new Paragraph("\n"));
                document.add(new Paragraph("问题 " + (answer.getQuestionIndex() + 1) +
                        " [" + (answer.getCategory() != null ? answer.getCategory() : "综合") + "]")
                        .setBold()
                        .setFontSize(12));
                document.add(new Paragraph("Q: " + sanitizeText(answer.getQuestion()))); // 打印具体题目
                document.add(new Paragraph("A: " + sanitizeText(answer.getUserAnswer() != null ? answer.getUserAnswer() : "未回答"))); // 打印用户回答，防止空指针
                document.add(new Paragraph("得分: " + answer.getScore() + "/100") // 打印单题得分
                        .setFontColor(getScoreColor(answer.getScore())));
                if (answer.getFeedback() != null) {
                    // 如果有单题评语，设置为斜体（Italic）打印展示
                    document.add(new Paragraph("评价: " + sanitizeText(answer.getFeedback()))
                            .setItalic());
                }
                if (answer.getReferenceAnswer() != null) {
                    // 如果系统存有标准参考答案，用高亮绿色（DeviceRgb: 39, 174, 96）单独渲染打印出来
                    document.add(new Paragraph("参考答案: " + sanitizeText(answer.getReferenceAnswer()))
                            .setFontColor(new DeviceRgb(39, 174, 96)));
                }
            }
        }

        document.close();
        return baos.toByteArray();
    }

    /**
     * 内部辅助方法：快速统一创建样式美观的二级小节标题。
     */
    private Paragraph createSectionTitle(String title) {
        return new Paragraph(title)
                .setFontSize(14)
                .setBold()
                .setFontColor(SECTION_COLOR)
                .setMarginTop(10);
    }

    /**
     * 内部辅助方法：将某一维度的评分作为新行（Row）添加进指定的表格（Table）中。
     */
    private void addScoreRow(Table table, String dimension, int score, int maxScore) {
        table.addCell(new Cell().add(new Paragraph(dimension)));
        table.addCell(new Cell().add(new Paragraph(score + " / " + maxScore)
                .setFontColor(getScoreColor(score * 100 / maxScore))));
    }

    /**
     * 内部颜色规则引擎：根据得分所处的区间段，智能返回不同的前景色，用以辅助强化视觉表现。
     */
    private DeviceRgb getScoreColor(int score) {
        if (score >= 80) return new DeviceRgb(39, 174, 96);   // 绿色
        if (score >= 60) return new DeviceRgb(241, 196, 15);  // 黄色
        return new DeviceRgb(231, 76, 60);                    // 红色
    }

    /**
     * 内部辅助转换方法：利用 Java 14 的新版 `switch` 表达式语法，将系统的面试状态枚举转换为友好的中文字符串。
     */
    private String getStatusText(InterviewSessionEntity.SessionStatus status) {
        return switch (status) {
            case CREATED -> "已创建";
            case IN_PROGRESS -> "进行中";
            case COMPLETED -> "已完成";
            case EVALUATED -> "已评估";
        };
    }
}
