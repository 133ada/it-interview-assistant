# AI Interview Platform（智能 AI 面试官平台）

> `app` 是仓库 `interview-guide` 的 Spring Boot 后端子项目，面向求职者提供简历分析、文字模拟面试、实时语音面试、面试计划、知识库 RAG 问答和多模型 Provider 管理能力。

本文档用于介绍当前项目的整体结构、技术栈、业务能力、主要接口、异步链路、配置方式、测试范围和部署方式。

## 1. 项目定位

这是一个模块化的 AI 面试平台后端，核心目标是把“准备简历 → 生成面试题 → 模拟面试 → AI 评估 → 查看报告”串成完整闭环，同时提供真实面试日程管理和面向技术资料的 RAG 知识库。

项目采用模块化单体架构：业务代码按领域拆分，基础设施和公共能力集中复用；对耗时 AI 调用、文档向量化和报告评估使用异步消息处理；对需要低延迟交互的语音面试使用 WebSocket。

当前 `app` 目录约包含：

- 193 个生产 Java 源文件；
- 28 个测试 Java 文件；
- 14 个 Prompt 模板；
- 10 个业务 Skill 目录，以及 `_shared` 共享参考资料。

上述数量会随代码变化，仅用于帮助理解项目规模。

## 2. 仓库层级结构

`app` 位于仓库根目录 `E:\interview-guide-master` 下。完整仓库还包含前端、Docker 编排、数据库初始化脚本和 Gradle Wrapper：

```text
interview-guide/
├─ app/                    # 当前后端子项目
├─ frontend/               # 前端项目
├─ docker/                 # PostgreSQL 初始化脚本等
├─ docs/                   # 仓库级文档
├─ gradle/                 # Gradle Wrapper 相关文件
├─ settings.gradle         # 根项目配置，include('app')
├─ gradlew / gradlew.bat   # Gradle Wrapper
├─ docker-compose.yml      # 完整系统编排
├─ docker-compose.dev.yml  # 开发环境编排
├─ .env.example            # 环境变量示例
└─ app/
   ├─ build.gradle
   ├─ Dockerfile
   ├─ src/
   └─ build/
```

## 3. `app` 目录结构

```text
app/
├─ build.gradle
├─ Dockerfile
├─ .gitignore
├─ src/
│  ├─ main/
│  │  ├─ java/interview/guide/
│  │  │  ├─ App.java
│  │  │  ├─ common/
│  │  │  ├─ infrastructure/
│  │  │  └─ modules/
│  │  └─ resources/
│  │     ├─ application.yml
│  │     ├─ application-local.yml
│  │     ├─ logback-spring.xml
│  │     ├─ prompts/
│  │     ├─ skills/
│  │     ├─ scripts/rate_limit_single.lua
│  │     ├─ voice-interview-opening.yml
│  │     └─ fonts/ZhuqueFangsong-Regular.ttf
│  └─ test/
│     ├─ java/               # 单元、Web、集成测试
│     └─ resources/          # 测试配置和测试文件
└─ build/                    # Gradle 构建产物
```

### 3.1 `build.gradle`

项目使用 Java 21 和 Gradle，主要配置包括：

- Spring Boot Web MVC、Validation、JPA、WebSocket、Actuator；
- Spring AI Chat、Embedding、pgvector；
- PostgreSQL，测试使用 H2；
- Apache Tika 文档解析；
- AWS S3 SDK，用于访问 RustFS/MinIO；
- Redisson、Redis Stream、Kafka；
- iText 8 PDF 导出和中文字体；
- DashScope SDK，用于 Qwen ASR/TTS；
- Lombok、MapStruct、SpringDoc OpenAPI；
- `bootRun` 自动读取仓库根目录 `.env`，并设置 UTF-8 JVM 编码。

### 3.2 `Dockerfile`

`Dockerfile` 使用多阶段构建：

1. 构建阶段使用 `gradle:8.14-jdk21`，解析依赖并执行 `:app:bootJar`。
2. 运行阶段使用 `eclipse-temurin:21-jre`，只复制可执行 JAR。
3. 容器暴露 `8080`，通过 `java -jar app.jar` 启动。

由于 Dockerfile 会引用仓库根目录的 `settings.gradle`、`gradlew`、`gradle/` 和 `app/`，应从仓库根目录通过 `docker-compose.yml` 构建，不建议把 `app` 单独作为 Docker build context。

## 4. 技术栈和组件

| 类别 | 技术 | 用途 |
|---|---|---|
| 编程语言 | Java 21 | 业务实现、Record、虚拟线程 |
| Web 框架 | Spring Boot 4.0、Spring MVC | REST API、文件上传、参数校验 |
| 实时通信 | Spring WebSocket | 语音面试双向通信 |
| 数据访问 | Spring Data JPA / Hibernate | Entity、Repository、事务持久化 |
| 主数据库 | PostgreSQL 16 | 业务数据存储 |
| 向量数据库 | pgvector | Embedding 存储和相似度检索 |
| AI 框架 | Spring AI 2.0 | Chat、Embedding、结构化输出 |
| AI 服务 | DashScope/Qwen、DeepSeek 等 | LLM、ASR、TTS、Embedding |
| 缓存与轻量消息 | Redis 7、Redisson | 缓存、分布式锁、Redis Stream、限流 |
| 消息队列 | Apache Kafka 3.7 | 简历分析异步任务 |
| 文件存储 | RustFS/MinIO S3 | 原始简历和知识库文件 |
| 文档解析 | Apache Tika | PDF、DOC、DOCX、TXT 解析 |
| PDF | iText 8 | 分析报告和面试报告导出 |
| API 文档 | SpringDoc OpenAPI | Swagger UI、OpenAPI JSON |
| 映射 | MapStruct | Entity/DTO 转换 |
| 测试 | JUnit 5、Spring Boot Test、H2 | 自动化测试 |

## 5. 系统拓扑

```text
                    ┌─────────────────────────────┐
                    │       frontend + Nginx       │
                    └──────────────┬──────────────┘
                                   │ HTTP / SSE / WebSocket
                    ┌──────────────▼──────────────┐
                    │      app（Spring Boot）      │
                    │ REST API + SSE + WebSocket   │
                    └──────┬─────────┬─────────┬───┘
                           │         │         │
              ┌────────────▼─┐ ┌────▼─────┐ ┌─▼────────────┐
              │ PostgreSQL   │ │ Redis    │ │ Kafka        │
              │ + pgvector  │ │ cache/MQ │ │ resume tasks │
              └──────────────┘ └──────────┘ └──────────────┘
                           │
                    ┌──────▼──────────────┐
                    │ RustFS / MinIO      │
                    │ S3-compatible files │
                    └─────────────────────┘

    app ── DashScope/OpenAI-compatible APIs ── LLM / Embedding / ASR / TTS
```

## 6. 核心业务模块

### 6.1 `modules.resume`：简历管理与 AI 分析

#### 主要功能

- 上传 PDF、DOC、DOCX、TXT 简历；
- 校验文件大小、扩展名和 MIME 类型；
- 使用 Apache Tika 抽取文本并清洗内容；
- 保存简历元数据、原文件、文件 Hash 和分析历史；
- 通过 Kafka 异步调用 LLM 生成结构化简历分析；
- 输出评分、优势、问题、改进建议等内容；
- 支持重新分析、查询详情、删除和 PDF 报告导出；
- 使用 Outbox、消息去重表、Redis 认领锁和 DLQ 提升可靠性。

#### 主要类

- `ResumeController`：对外 REST 接口；
- `ResumeUploadService`：上传流程编排；
- `ResumeParseService`：文档解析；
- `ResumeGradingService`：调用 LLM 分析简历；
- `AnalyzeKafkaProducer` / `AnalyzeKafkaConsumer`：Kafka 生产和消费；
- `ResumeAnalyzeOutboxService` / `ResumeAnalyzeOutboxRelay`：Outbox 和补偿投递；
- `ResumeAnalyzeDlqHandler`：失败消息处理；
- `ResumePersistenceService`、`ResumeHistoryService`：数据和历史管理。

#### 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/resumes/upload` | 上传并创建分析任务 |
| GET | `/api/resumes` | 查询简历列表 |
| GET | `/api/resumes/{id}/detail` | 查询详情和分析结果 |
| GET | `/api/resumes/{id}/export` | 导出分析 PDF |
| POST | `/api/resumes/{id}/reanalyze` | 重新分析 |
| DELETE | `/api/resumes/{id}` | 删除简历及关联数据 |
| GET | `/api/resumes/health` | 模块健康检查 |

### 6.2 `modules.interview`：文字模拟面试

#### 主要功能

- 根据技能、难度、简历内容和配置生成题目；
- 创建、查询和恢复面试会话；
- 获取当前题目、提交答案、保存答案草稿；
- 保存题目分类、回答、评分、反馈、参考答案和关键点；
- 完成面试后，通过 Redis Stream 异步生成评价报告；
- 生成逐题评价、总体评分、优势、改进建议和参考答案；
- 通过补偿任务处理消息未投递、任务卡住或租约失效；
- 支持历史详情和 PDF 导出。

#### 主要类

- `InterviewController`：面试会话 API；
- `InterviewSkillController` / `InterviewSkillService`：技能和 JD 解析；
- `InterviewQuestionService`：AI 出题；
- `InterviewSessionService`：面试流程；
- `AnswerEvaluationService`：答案评价；
- `InterviewPersistenceService`：会话、答题和状态持久化；
- `EvaluateStreamProducer` / `EvaluateStreamConsumer`：评估异步链路；
- `EvaluateCompensationJob`：评估补偿。

#### 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/POST | `/api/interview/sessions` | 查询或创建会话 |
| GET | `/api/interview/sessions/{sessionId}` | 查询会话 |
| GET | `/api/interview/sessions/{sessionId}/question` | 获取当前题目 |
| POST | `/api/interview/sessions/{sessionId}/answers` | 提交答案 |
| PUT | `/api/interview/sessions/{sessionId}/answers` | 保存答案草稿 |
| POST | `/api/interview/sessions/{sessionId}/complete` | 完成面试 |
| GET | `/api/interview/sessions/{sessionId}/report` | 获取报告 |
| GET | `/api/interview/sessions/{sessionId}/details` | 获取答题明细 |
| GET | `/api/interview/sessions/{sessionId}/export` | 导出面试 PDF |
| GET | `/api/interview/sessions/unfinished/{resumeId}` | 查找未完成会话 |
| DELETE | `/api/interview/sessions/{sessionId}` | 删除会话 |
| GET | `/api/interview/skills` | 技能列表 |
| GET | `/api/interview/skills/{id}` | 技能详情 |
| POST | `/api/interview/skills/parse-jd` | 解析职位描述 |

### 6.3 `modules.voiceinterview`：实时语音面试

#### 实时链路

```text
浏览器麦克风音频
    │ WebSocket /ws/voice-interview/{sessionId}
    ▼
VoiceInterviewWebSocketHandler
    ├─ QwenAsrService：实时语音识别、partial/final 字幕、VAD
    ├─ DashscopeLlmService：根据上下文生成问题
    ├─ QwenTtsService：文本转语音
    └─ VoiceInterviewService：会话和消息持久化
```

#### 主要功能

- 创建、查询、结束、暂停、恢复和删除语音会话；
- 支持 INTRO、TECH、PROJECT、HR 四个阶段；
- 可配置阶段的时长、题目数和是否启用；
- WebSocket 接收音频和控制消息；
- 返回欢迎消息、实时字幕、最终字幕、AI 文本和音频分片；
- Qwen3 Realtime ASR 支持中文识别、Server VAD、静音阈值和重连；
- LLM 支持流式生成，可按 token 或句子推送；
- Qwen3 Realtime TTS 将 AI 回复合成为 PCM 音频；
- Redis 缓存活跃会话，断线时自动结束异常残留会话；
- 对会话、IP 和全局并发数限流；
- 会话结束后通过 Redis Stream 异步生成语音面试评价。

#### HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/voice-interview/sessions` | 创建语音会话并返回 WebSocket 地址 |
| GET | `/api/voice-interview/sessions/{sessionId}` | 查询会话 |
| PUT | `/api/voice-interview/sessions/{sessionId}/pause` | 暂停会话 |
| PUT | `/api/voice-interview/sessions/{sessionId}/resume` | 恢复会话 |
| POST | `/api/voice-interview/sessions/{sessionId}/end` | 结束会话 |
| GET | `/api/voice-interview/sessions` | 会话列表 |
| GET | `/api/voice-interview/sessions/{sessionId}/messages` | 对话消息 |
| GET | `/api/voice-interview/sessions/{sessionId}/evaluation` | 查询评价状态/结果 |
| POST | `/api/voice-interview/sessions/{sessionId}/evaluation` | 触发评价 |
| DELETE | `/api/voice-interview/sessions/{sessionId}` | 删除会话 |

#### WebSocket

```text
ws://localhost:8080/ws/voice-interview/{sessionId}
```

控制消息使用 `type=control`，动作包括 `start_phase`、`end_phase`、`end_interview`、`submit` 等；字幕消息使用 `type=subtitle`，并区分临时识别结果和最终识别结果。字段定义以 `dto/WebSocketControlMessage`、`dto/WebSocketSubtitleMessage` 为准。

### 6.4 `modules.knowledgebase`：知识库与 RAG

#### 文档处理流程

```text
上传文档 → Tika 解析 → 文本清洗/切分 → Embedding
        → PostgreSQL vector_store（pgvector）
        → 相似度检索 → Prompt 组装 → LLM 流式回答
```

#### 主要功能

- 上传、下载、删除和搜索知识库文档；
- 记录文档分类、类型、大小、Hash、解析文本和向量状态；
- 使用 Redis Stream 异步向量化；
- 使用 pgvector HNSW 索引和余弦距离检索；
- 根据查询长度选择不同的 `topK` 和相似度阈值；
- 查询前可使用 LLM 改写问题，改善召回质量；
- 支持普通查询和 SSE 流式查询；
- 支持 RAG 聊天会话、标题、置顶、绑定知识库、历史消息和流式回答；
- 支持手动重新向量化。

#### 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/knowledgebase/list` | 文档列表 |
| POST | `/api/knowledgebase/upload` | 上传文档 |
| GET | `/api/knowledgebase/{id}` | 查询文档 |
| GET | `/api/knowledgebase/{id}/download` | 下载原文件 |
| DELETE | `/api/knowledgebase/{id}` | 删除文档 |
| GET | `/api/knowledgebase/search?keyword=...` | 关键字搜索 |
| GET | `/api/knowledgebase/stats` | 统计信息 |
| GET | `/api/knowledgebase/categories` | 分类列表 |
| PUT | `/api/knowledgebase/{id}/category` | 修改分类 |
| POST | `/api/knowledgebase/query` | 普通 RAG 查询 |
| POST | `/api/knowledgebase/query/stream` | RAG 流式查询 |
| POST | `/api/knowledgebase/{id}/revectorize` | 重新向量化 |
| POST | `/api/rag-chat/sessions` | 创建 RAG 会话 |
| GET | `/api/rag-chat/sessions` | RAG 会话列表 |
| GET | `/api/rag-chat/sessions/{sessionId}` | RAG 会话详情 |
| PUT | `/api/rag-chat/sessions/{sessionId}/title` | 修改标题 |
| PUT | `/api/rag-chat/sessions/{sessionId}/pin` | 置顶/取消置顶 |
| PUT | `/api/rag-chat/sessions/{sessionId}/knowledge-bases` | 绑定知识库 |
| POST | `/api/rag-chat/sessions/{sessionId}/messages/stream` | 流式发送消息 |
| DELETE | `/api/rag-chat/sessions/{sessionId}` | 删除 RAG 会话 |

### 6.5 `modules.interviewschedule`：面试日程

该模块用于管理真实面试安排：

- 记录公司、职位、面试时间、面试类型、会议链接、轮次、面试官和备注；
- 支持从原始文本解析日程；
- 支持创建、查询、列表筛选、更新、删除和状态变更；
- `ScheduleStatusUpdater` 定时更新过期日程状态。

接口前缀为 `/api/interview-schedule`：

```text
POST   /api/interview-schedule/parse
POST   /api/interview-schedule
GET    /api/interview-schedule
GET    /api/interview-schedule/{id}
PUT    /api/interview-schedule/{id}
PATCH  /api/interview-schedule/{id}/status
PUT    /api/interview-schedule/{id}/status
DELETE /api/interview-schedule/{id}
```

### 6.6 `modules.llmprovider`：模型 Provider 配置

该模块把 LLM 配置从业务代码中抽离，支持运行时管理：

- Provider 列表、详情、创建、更新、删除和连通性测试；
- 设置默认 Chat Provider 和默认 Embedding Provider；
- Provider 配置热加载；
- API Key 加密存储；
- 将运行时可写配置保存到用户目录下的 YAML/ENV 文件；
- 管理 ASR/TTS 配置，并提供 ASR 测试；
- `LlmProviderRegistry` 负责 Provider 快照和运行时解析。

接口前缀为 `/api/llm-provider`，主要路径如下：

```text
GET/POST/PUT/DELETE /api/llm-provider...
POST /api/llm-provider/{id}/test
POST /api/llm-provider/reload
GET/PUT /api/llm-provider/default-provider
GET/PUT /api/llm-provider/default-embedding-provider
GET/PUT /api/llm-provider/voice/asr
GET/PUT /api/llm-provider/voice/tts
POST /api/llm-provider/voice/asr/test
```

## 7. 公共能力与基础设施

### 7.1 `common/ai`

- `LlmProviderRegistry`：解析当前 Chat/Embedding Provider；
- `ApiPathResolver`：兼容不同 OpenAI 协议 Provider 的路径差异；
- `StructuredOutputInvoker`：调用 LLM 并解析结构化 JSON；
- `PromptSanitizer`、`PromptSecurityConstants`：清理输入，降低 Prompt 注入和上下文污染风险；
- `AgentUtilsConfiguration`、`AgentUtilsProperties`：加载 `resources/skills`。

启动类显式排除了 Spring AI 默认 OpenAI 自动配置，改由项目自己的 Provider 配置和 Registry 管理模型客户端。

### 7.2 `common/evaluation`

- `UnifiedEvaluationService`：统一文字面试和语音面试的评价结构；
- `EvaluationReport`、`QaRecord`：报告、问题评分、参考答案和关键点模型；
- `InterviewEvaluationProperties`：评价批量大小及参数配置。

### 7.3 `common/config`

集中管理：

- 应用参数和异步消费者数量；
- CORS、Jackson、OpenAPI；
- LLM、Embedding 和 pgvector；
- S3/RustFS 存储客户端；
- 配置属性绑定。

### 7.4 `common/async`、`common/aspect`、`common/exception`

- `AbstractStreamProducer` / `AbstractStreamConsumer`：封装 Redis Stream 生产、消费和确认；
- `@RateLimit` + `RateLimitAspect`：方法级限流；
- `rate_limit_single.lua`：Redis 原子限流脚本；
- `GlobalExceptionHandler`：统一处理参数校验、业务异常、限流异常和系统异常；
- `Result<T>`、`ErrorResult`：统一 API 返回结构。

### 7.5 `infrastructure`

| 子包 | 作用 |
|---|---|
| `file` | MIME 检测、文件校验、Hash、Tika 解析、文本清洗、文件存储 |
| `export` | iText 中文 PDF 导出 |
| `kafka` | Kafka 连接、Topic 和消费者配置 |
| `redis` | Redisson、缓存、锁和 Redis 操作 |
| `mapper` | 各领域 Entity/DTO 映射 |

## 8. Prompt、Skill 与静态资源

### 8.1 Prompt 模板

`src/main/resources/prompts/` 使用 StringTemplate 风格模板：

- `resume-analysis-*`：简历分析；
- `jd-parse-system.st`：职位描述解析；
- `interview-question-resume-*`：结合简历出题；
- `interview-question-skill-*`：结合技能出题；
- `interview-evaluation-*`：逐题评价；
- `interview-evaluation-summary-*`：整体总结；
- `knowledgebase-query-*`：RAG 查询改写、检索和回答。

### 8.2 Skill 资源

`src/main/resources/skills/` 中包含：

- `java-backend`、`java-backend-tencent`；
- `python-backend`；
- `frontend`；
- `algorithm`；
- `system-design`；
- `test-development`；
- `ai-agent-dev`；
- `ali-backend`；
- `bytedance-backend`；
- `_shared/references`：Java、Spring、数据库、Redis、消息队列、分布式、系统设计和前端等资料。

每个 Skill 通常包含 `SKILL.md` 和 `skill.meta.yml`，供 Agent/面试题生成逻辑按技能加载。

## 9. 数据模型与异步架构

### 9.1 主要实体

| 领域 | 实体 |
|---|---|
| 简历 | `ResumeEntity`、`ResumeAnalysisEntity`、`ResumeAnalyzeOutboxEntity`、`ResumeMessageDedupEntity` |
| 文字面试 | `InterviewSessionEntity`、`InterviewAnswerEntity` |
| 语音面试 | `VoiceInterviewSessionEntity`、`VoiceInterviewMessageEntity`、`VoiceInterviewEvaluationEntity` |
| 知识库 | `KnowledgeBaseEntity`、pgvector 默认 `vector_store` |
| RAG | `RagChatSessionEntity`、`RagChatMessageEntity` |
| 面试日程 | `InterviewScheduleEntity` |
| Provider | `LlmProviderEntity`、`LlmGlobalSettingEntity` |

### 9.2 消息分工

- **Kafka：简历分析**。上传事务写入 Outbox，Relay 定时投递；Consumer 处理任务，支持认领锁、幂等、重试和 DLQ。
- **Redis Stream：文字面试评估**。完成面试后投递评估消息，Consumer 批量调用 LLM，补偿 Job 负责恢复异常状态。
- **Redis Stream：知识库向量化**。文档上传后异步解析和写入 pgvector，支持重向量化。
- **Redis Stream：语音面试评估**。会话结束后异步生成报告，不阻塞实时通话。

### 9.3 可靠性设计

1. Outbox 防止数据库提交成功但消息没有投递；
2. Redis 分布式锁减少重复消费；
3. 数据库唯一约束和去重表提供幂等兜底；
4. 持久化任务状态和错误信息，便于前端查询；
5. 定时任务重新投递 Pending 任务并恢复卡住的 Processing 任务；
6. 部分链路在 Redis 故障时可降级为数据库约束兜底。

## 10. 配置说明

### 10.1 配置文件

- `src/main/resources/application.yml`：默认配置，默认激活 `local` Profile，同时包含容器环境的环境变量占位符；
- `src/main/resources/application-local.yml`：本地开发配置，依赖通常通过 `localhost` 访问；
- `src/test/resources/application-test.yml`：测试配置，使用 H2；
- `src/main/resources/voice-interview-opening.yml`：语音面试开场语；
- `src/main/resources/logback-spring.xml`：日志格式和输出配置。

### 10.2 必需依赖服务

本地完整运行通常需要：

- PostgreSQL 16 + pgvector；
- Redis 7；
- Kafka；
- MinIO 或 RustFS；
- 至少一个可用的 LLM Provider；
- 语音功能还需要 ASR/TTS 权限以及可访问 DashScope WebSocket 的网络环境。

### 10.3 常用环境变量

请复制仓库根目录 `.env.example` 为 `.env`，并替换为自己的值：

```dotenv
AI_BAILIAN_API_KEY=your_dashscope_api_key
AI_MODEL=qwen3.5-flash

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=interview_guide
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

REDIS_HOST=localhost
REDIS_PORT=6379
KAFKA_BOOTSTRAP_SERVERS=localhost:29092

APP_STORAGE_ENDPOINT=http://localhost:9000
APP_STORAGE_ACCESS_KEY=minioadmin
APP_STORAGE_SECRET_KEY=change_me
APP_STORAGE_BUCKET=interview-guide

APP_AI_CONFIG_ENCRYPTION_KEY=change_me
APP_AI_CONFIG_REQUIRE_ENCRYPTION_KEY=false
```

此外还支持 `PROVIDER_*_API_KEY`、`PROVIDER_*_MODEL`、语音阶段参数、ASR/TTS 参数、RAG 检索参数、Kafka 参数、Redis Stream 消费者数量等环境变量。完整配置以 `application.yml`、`application-local.yml` 和仓库根目录 `.env.example` 为准。

### 10.4 安全注意事项

- 不要把真实 API Key、数据库密码或对象存储密钥提交到 Git；
- 生产环境使用环境变量、密钥管理服务或外部配置文件；
- 开启 `APP_AI_CONFIG_REQUIRE_ENCRYPTION_KEY=true` 时必须提供加密密钥；
- 生产环境建议关闭 `ddl-auto: update` 和向量表自动初始化，改用数据库迁移；
- 生产环境应重新审核 CORS、Swagger、对象存储访问权限以及日志中的用户内容。

## 11. 本地运行

### 11.1 准备环境

- JDK 21；
- Docker Desktop / Docker Engine + Compose；
- 可访问 Maven 仓库和 AI Provider；
- 配置仓库根目录 `.env`。

### 11.2 启动依赖服务

在 `E:\interview-guide-master` 执行：

```powershell
docker compose up -d postgres redis kafka minio createbuckets
```

启动完整系统：

```powershell
docker compose up --build
```

### 11.3 启动后端

在仓库根目录执行：

```powershell
.\gradlew.bat :app:bootRun
```

也可以在 `app` 目录执行：

```powershell
gradle bootRun
```

默认端口为 `8080`。

### 11.4 文档和健康检查

- Swagger UI：`http://localhost:8080/swagger-ui.html`；
- OpenAPI JSON：`http://localhost:8080/v3/api-docs`；
- 简历模块健康检查：`GET /api/resumes/health`；
- Actuator：项目已引入，具体暴露端点以当前配置为准。

## 12. 构建、测试和部署

### 构建

```powershell
.\gradlew.bat :app:build
.\gradlew.bat :app:bootJar
```

可执行 JAR 默认生成在：

```text
E:\interview-guide-master\app\build\libs\app-0.0.1-SNAPSHOT.jar
```

### 测试

```powershell
.\gradlew.bat :app:test
```

测试覆盖：

- 应用启动和基础上下文；
- Provider 路径解析、注册表和配置管理；
- Redis Lua 限流脚本、限流切面和限流异常；
- PDF/DOC/TXT 文档解析、文件清洗和集成流程；
- 文字面试 Controller、DTO 和流程；
- 知识库向量服务；
- 简历 Kafka Producer/Consumer、Outbox、持久化和失败处理；
- 语音面试集成、会话状态、Prompt、ASR、TTS、LLM、暂停恢复。

测试使用 `src/test/resources/application-test.yml`，数据库为内存 H2。涉及 Redis、Kafka、外部 AI API 或 WebSocket 的测试，需要按测试类型准备相应环境或使用 Mock。

## 13. 典型业务流程

### 13.1 简历到模拟面试

```text
上传简历
  → 文件校验和 Tika 解析
  → 保存简历 + Outbox
  → Kafka 异步分析
  → 保存评分/优势/改进建议
  → 选择技能和难度
  → AI 生成题目
  → 逐题回答并保存
  → Redis Stream 异步评估
  → 查看报告 / 导出 PDF
```

### 13.2 知识库 RAG

```text
上传 PDF/DOCX/TXT
  → 解析和清洗
  → Redis Stream 向量化
  → Embedding 写入 pgvector
  → 用户提问
  → 可选问题改写
  → 相似度检索
  → 组装上下文
  → LLM SSE 流式回答
  → 保存 RAG 会话和消息
```

### 13.3 实时语音面试

```text
创建会话
  → 建立 WebSocket
  → 连接 Qwen ASR
  → 浏览器上传音频
  → 返回实时/最终字幕
  → LLM 生成面试官问题
  → 先返回文本，再返回 TTS 音频分片
  → 阶段切换
  → 结束会话
  → Redis Stream 异步生成评价
```

## 14. 代码阅读顺序

建议按以下顺序熟悉项目：

1. `src/main/java/interview/guide/App.java`：启动方式、定时任务、AI 自动配置排除；
2. `src/main/resources/application.yml`：数据库、Redis、Kafka、存储、AI、RAG、语音配置；
3. 各模块 Controller：建立接口入口；
4. 各模块 Entity 和 Repository：理解数据模型；
5. `ResumeUploadService`、`InterviewSessionService`、`KnowledgeBaseQueryService`、`VoiceInterviewWebSocketHandler`：掌握主链路；
6. Producer/Consumer、Outbox、补偿 Job、限流切面和公共 AI 封装：理解可靠性和横切能力。

## 15. 当前边界与维护建议

- `app` 主要承担后端业务，前端页面和 Nginx 配置在 `frontend`；
- 当前代码中未见独立的用户认证/授权模块，公网部署前应补充认证、权限、租户隔离和数据访问控制；
- 语音功能依赖浏览器音频格式、网络延迟、DashScope WebSocket 和前端协议实现；
- 本地 `bootRun` 和容器运行时的依赖地址不同，前者通常使用 `localhost`，后者使用 Compose 服务名；
- 生产环境建议增加数据库迁移、密钥管理、监控告警、链路追踪、消息重试和更严格的上传安全检查。

---

**文档路径**：`E:\interview-guide-master\app\readme.md`  
**文档生成日期**：2026-09-01  
**项目默认端口**：`8080`
