# 数据库、缓存与消息队列的数据一致性总结

> 整理日期：2026-08-24
> 来源：`interview.guide.modules.resume`（简历分析 Kafka 链路）代码走读 + 问答总结
> 关联代码：`listener/AnalyzeKafkaProducer|Consumer`、`listener/ResumeAnalyzeDlqHandler`、`service/ResumeAnalyzeOutboxService|Relay`、`service/ResumePersistenceService`、`repository/ResumeAnalyzeOutboxRepository|ResumeMessageDedupRepository`、`infrastructure/kafka/KafkaConfig|KafkaProperties`

---

## 一、核心矛盾：传统后端的分布式数据一致性

在传统后端项目中，数据库、缓存和消息队列之间最常见的问题，本质上都是**分布式系统中的数据一致性问题**：

- **数据库**负责数据最终落盘，是数据一致性的基准；
- **缓存**用于提升访问性能，但容易出现**缓存与数据库数据不一致、缓存穿透、击穿和雪崩**等问题；
- **消息队列**用于系统解耦和异步处理，但可能带来**消息重复消费、消息丢失、消息顺序以及最终一致性**等问题。

三者结合使用时，还需要重点解决**数据库事务与缓存更新、数据库事务与消息发送之间的一致性**，例如经典的"先更新数据库还是先删除缓存"、以及"本地事务成功但消息发送失败"等场景。

因此，传统后端架构的核心难点之一，就是在**性能、可用性和数据一致性**之间进行权衡。

---

## 二、缓存与数据库（经典问题清单）

| 问题 | 现象 | 常见对策 |
|------|------|----------|
| **缓存穿透** | 查询不存在的数据，请求直达 DB | 布隆过滤器 / 缓存空值（短 TTL）/ 参数校验 |
| **缓存击穿** | 热点 key 过期瞬间，并发打到 DB | 互斥锁（缓存重建串行化）/ 逻辑过期 |
| **缓存雪崩** | 大量 key 同时过期 / Redis 宕机，DB 被打垮 | 过期时间加随机抖动 / 多级缓存 / 熔断限流降级 |
| **缓存与 DB 不一致** | 更新 DB 后缓存还是旧值 | 先更新 DB 再删缓存（Cache Aside）+ 延迟双删 / 消息队列异步删除 + 兜底 |

> 核心：**Cache Aside（旁路缓存）**——读走缓存、写先落库再删缓存；删除失败靠延迟重试/消息补偿，最终一致。

---

## 三、消息队列经典问题

| 问题 | 产生原因 | 解决思路 |
|------|----------|----------|
| **消息重复消费** | Kafka at-least-once / 消费成功但 ACK 前崩溃重投 | 业务幂等（DB 唯一键 / 幂等表 / 状态机） |
| **消息丢失** | 生产者异步发送失败 / broker 单副本丢失 / 消费失败直接提交 | 生产者 acks=all + 幂等生产者 / 副本 & ISR / 手动 ACK + 失败重试 → DLQ |
| **消息顺序** | 多分区并行消费 / 重试乱序 | 同一 key 进同一分区 + 单线程消费 / 失败不越序（原地 seek 重试） |
| **最终一致性** | 本地事务成功但消息发送失败 | **事务性 Outbox**：同事务写业务表 + Outbox 表，中继异步补偿投递 |
| **消息安全** | 数据泄露 / 反序列化攻击 / 无认证 | topic ACL + TLS + PII 最小化 / 序列化白名单 / 消息签名 |

> 核心：MQ 通常只承诺 **at-least-once**，最终一致性与幂等消费是配套的——**宁可重复，不可丢失**。

---

## 四、DB 事务 与 MQ 发送的一致性（事务性 Outbox）

### 问题场景
"本地事务成功但消息发送失败"：业务写入 DB 成功，但 Kafka 发送失败 → 下游永远收不到消息。

### 方案：Transactional Outbox（本项目采用）
1. **同事务写入**：业务表 + `resume_analyze_outbox`（PENDING）在同一 DB 事务内原子提交，保证"DB 已改 → 必有一行待投递消息"；
2. **中继补偿投递**：定时任务扫描 PENDING 行投 Kafka，成功才守卫式迁移 SENT；
3. **失败留待重试**：投递失败保留 PENDING，重试计数 +1，超上限标记 FAILED（人工排查）；
4. **死信收口**：投递重试耗尽 → 写 DLQ 保留现场 + 回写业务状态 FAILED。

### 为什么不在 DB 事务内直接发消息？
- 违反"禁止事务内调用外部 API"（长事务占 DB 连接）；
- 事务回滚会导致"已发出但业务没落库"的脏消息。

---

## 五、本项目落地实践（简历分析 Kafka 链路）

### 5.1 总体架构

```
上传 / reanalyze
   │  ResumePersistenceService.saveResume / ResumeUploadService.reanalyze（@Transactional）
   ▼
[ resume 行 + resume_analyze_outbox(PENDING) ]   ← 同一 DB 事务原子提交（Transactional Outbox）
   │
   ▼  ResumeAnalyzeOutboxRelay（@Scheduled 5s 轮询）→ 投 Kafka → guard 迁移 SENT
[ Kafka resume-analyze topic ]  key=resumeId，1 分区
   │
   ▼  AnalyzeKafkaConsumer（手动 ACK，成功才提交 offset）
[ LLM 分析 → recordAnalysisResult() 同一事务写：幂等行 + 分析记录 + COMPLETED ]
   │ 失败 → DefaultErrorHandler 指数退避重试 3 次 → DLQ → 标记 FAILED
```

### 5.2 DB 与 Kafka 一致性（如何保证）

- **同事务写库 + 写 Outbox**：`ResumePersistenceService.saveResume`（`:73`）事务内保存简历后调用 `outboxService.createTask`（`ResumeAnalyzeOutboxService.java:37`，REQUIRED 传播，加入调用方事务）。`reanalyze`（`ResumeUploadService.java:169`）同样：重置状态 PENDING + 建 Outbox 行。
- **中继补偿**：`ResumeAnalyzeOutboxRelay.pollPendingOutbox()`（`:53`）按 id 升序拉 PENDING 行 → 投 Kafka → 守卫式迁移 `updateStatusIf(id, SENT, null, PENDING)`（`ResumeAnalyzeOutboxRepository.java:32`，`WHERE status=PENDING`，防多实例覆盖）。
- **最终一致 + at-least-once**：投递成功但 `markSent` 前崩溃 → 行仍是 PENDING → 下轮重投**同一 messageId** → 消费端幂等兜底。
- **死信收口**：producer 内部 `retries=3` + `delivery.timeout.ms`，叠加业务层 `sendMaxRetries=2` 指数退避（`AnalyzeKafkaProducer.java:64`）；全部失败 → 同步写 DLQ（`sendToDlqSync`）→ 回写 DB FAILED；DLQ 也失败 → 抛 `BusinessException` → 中继保留 PENDING 下轮重试。
- **中继非事务**（`ResumeAnalyzeOutboxRelay.java:25`）：Kafka 发送不在 DB 事务内，符合"禁止事务内调用外部 API"，避免长事务占连接。

### 5.3 幂等消费（四层防御）

1. **生产者幂等**：`ENABLE_IDEMPOTENCE_CONFIG=true + acks=all + retries=3`（`KafkaConfig.java:66`），Kafka 序列号机制保证 producer 内部重试不产生重复消息。
2. **DB 唯一约束兜底（最终防线）**：`resume_message_dedup.message_id` 唯一索引（`ResumeMessageDedupEntity.java:26`）。`recordAnalysisResult`（`ResumePersistenceService.java:150`）在**同一事务**内：插幂等行 → `flush()` 立即触发唯一约束 → 存分析结果 → 守卫式更新 COMPLETED。
3. **SQLState 精确区分**（`AnalyzeKafkaConsumer.java:176`）：23505（唯一约束=重复）/ 23503（FK=简历已删）→ **ACK 跳过**；其余完整性异常（22001 等）→ 重抛走重试→DLQ，避免把"消息缺陷"误判为"已处理"。
4. **认领锁 + 幂等快路径**：`existsByMessageId` 前置检查避免重复调 LLM；Redis 分布式锁 `resume:analyze:claim:{messageId}` 串行化并发；**有界等待 + 超时降级**（`:119`）——抢不到锁不"跳过"，降级无锁处理让 DB 唯一约束兜底，**宁可重复一次 LLM，不丢消息**；Redis 不可用同样降级。

> 真实语义：最坏情况（锁降级 + 并发）同一消息被两个消费者各调一次 LLM，但只落一份结果（唯一约束），且不丢消息。

### 5.4 顺序消费

当前配置为**强顺序（全局有序）**：

- Topic **1 分区**（`KafkaConfig.java:188`）→ Kafka 保证分区内有序；
- **key = resumeId**（`AnalyzeKafkaProducer.java:92`）→ 同一简历进同一分区；
- **concurrency = 1**（`KafkaProperties.java:41`）→ 单线程逐条消费；
- **失败不越序**：`DefaultErrorHandler` 将失败 record `seek` 回去、退避期间暂停容器；手动 ACK 成功才提交 offset。

**代价**：单分区 + 单线程 = **队头阻塞**，一条慢消息阻塞整条队列；顺序性用吞吐换。将来加分区后可保留"逐简历有序"（key 路由），仅丢失跨简历全局顺序。

**已知乱序边界（Outbox 跨轮重试）**：任务 A 投递失败保留 PENDING，期间 reanalyze 生成任务 B；下轮 B 先投成功、A 后投成功 → Kafka 里 B 在 A 前 → 旧快照 A 的分析记录仍会落库，且 `getLatestAnalysis`（按 analyzedAt DESC，`ResumePersistenceService.java:202`）可能返回旧快照结果。状态本身有 `updateAnalyzeStatus(PROCESSING→COMPLETED)` 守卫（`ResumeRepository.java:47`）防回退，但"最新分析"可能来自旧消息。缓解：同一 resumeId 只允许一个非终态 Outbox 行，或任务带版本号仅在版本不落后时落库。

### 5.5 MQ 安全性检查清单（针对本模块）

| # | 风险 | 现状 / 建议 |
|---|------|-------------|
| 1 | **PII 落 Kafka** | 消息体是完整简历文本，DLQ 同样带；建议只投 `resumeId+messageId` 消费端取 DB，或开 ACL + TLS + 端到端加密 |
| 2 | **Jackson 多态反序列化** | `KafkaConfig.java:96` 白名单写的是 `.listener`，消息类实际在 `.model` 包——**包前缀不匹配，需改为 `.model` 并补序列化往返集成测试** |
| 3 | **无认证/防篡改** | bootstrap 默认 `localhost:9092` 明文；生产要 SASL_SSL + topic ACL |
| 4 | **单副本 + 自动建 topic** | `replicationFactor=1`，acks=all 退化为单副本同步；建议 RF≥2 + `min.insync.replicas` 对齐 + 固定 topic 清单 |
| 5 | **重试放大 / 队头阻塞** | 生产者 3+2 次重试最坏 ~20s；消费者 3 次退避 7s 阻塞队列（DLQ 收口正确，`setCommitRecovered(true)` 防无限重投） |
| 6 | **表无界增长** | Outbox 的 SENT/DLQ/FAILED 行、dedup 行无清理任务；reanalyze 每次新增 analysis 行；建议周期归档清理 |
| 7 | **LLM 提示注入（顺带）** | 简历文本是攻击者可控输入，直接拼进 prompt（`ResumeGradingService.java:98`） |

### 5.6 优先级最高的改进

1. `KafkaConfig.java:96` 白名单包改为 `.model` + 序列化往返集成测试（当前可能是暗雷）；
2. 消息体去 PII + Kafka SASL_SSL / ACL / RF≥2；
3. Outbox 表清理任务 + 同一 resumeId 仅允许单 in-flight 任务（消除乱序窗口与表膨胀）。

---

## 六、权衡总结

- **一致性 vs 性能**：事务性 Outbox 用"DB 状态驱动补偿投递"换取最终一致，代价是一段 5s 轮询延迟；
- **顺序 vs 吞吐**：单分区 + 单线程保证全局有序，代价是队头阻塞，无法并行；
- **幂等 vs 成本**：DB 唯一约束 + 认领锁在"不重复调 LLM"与"不丢消息"之间折中，最坏情况容忍一次重复 LLM；
- **安全 vs 便利**：序列化白名单、topic ACL、PII 最小化都是有意识的取舍，需在部署时落实。

> 一句话总结：传统后端的一致性难点不在单一组件，而在**跨组件的事务边界**——用"本地事务 + Outbox + 幂等消费 + 最终一致"的组合，在性能、可用性、一致性三者间取得可控的权衡。
