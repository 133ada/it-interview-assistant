# 面试模块 数据库 · Redis · 消息队列 数据一致性总结

> 整理日期：2026-08-24
> 来源：`interview.guide.modules.interview` 代码走读 + 问答总结
> 关联代码：`listener/EvaluateStreamProducer|Consumer`、`service/EvaluateCompensationJob`、`service/InterviewSessionService|InterviewPersistenceService`、`infrastructure/redis/InterviewSessionCache|RedisService`、`common/async/AbstractStreamProducer|Consumer`、`common/constant/AsyncTaskStreamConstants`、`repository/InterviewSessionRepository`

---

## 一、总体架构：Redis Stream 消息队列（非 Kafka）

与简历模块（Kafka + 事务性 Outbox）不同，面试模块的异步评估链路用 **Redis Stream + 消费者组** 实现：

```
提交最后一题 / 提前交卷 / 生成报告
   │
   ▼  ① 先写 DB 状态（evaluateStatus=PENDING）    ← 状态先行（区别于 Outbox）
   ▼  ② XADD 消息到 interview:evaluate:stream      ← 消息后投
   │
   ▼  EvaluateStreamConsumer（消费者组 evaluate-group，默认单线程）
   │     tryAcquire: DB  FOR UPDATE CAS  PENDING→PROCESSING   ← 幂等 + 并发串行
   │     LLM 评估 → saveReport（EVALUATED + 报告落库，同一事务）→ 删 Redis 缓存
   │     失败 → retryMessage：重置 PENDING + XADD 新消息（指数退避 dueAt）
   │     重试耗尽 → markFailed(FAILED) + 写入 DLQ（interview:evaluate:dlq）
   │
   ▼  EvaluateCompensationJob（每 60s）兜底最终一致
       · 滞留 5min 仍 PENDING → 重新投递
       · 租约过期（30min）仍 PROCESSING → 双检锁重置 PENDING → 重新投递
```

**核心思想**：以 **DB 的 `evaluateStatus` 字段作为任务生命周期的权威状态（source of truth）**，MQ 只负责"通知"，丢失与重复都由 DB 状态 + 补偿任务兜底——这是与 Outbox 不同的另一种最终一致方案。

---

## 二、DB ↔ 消息队列 的一致性

### 2.1 状态先行 + 消息后投（写前状态）

`submitAnswer` 最后一题（`InterviewSessionService.java:421`）：

```java
persistenceService.updateEvaluateStatus(sessionId, AsyncTaskStatus.PENDING, null); // ① DB 先置 PENDING
evaluateStreamProducer.sendEvaluateTask(sessionId);                                // ② MQ 后投
```

- 注释明确：*"MySQL 中 evaluateStatus=PENDING 是消费者判断任务的依据，必须先于 MQ 投递写入。若此步失败：抛异常，MQ 消息不会发送，不会触发评估，状态一致。"*
- `completeInterview` 同理：`completeInterview()` 在 **REQUIRES_NEW 独立事务**内原子完成 `status→COMPLETED + evaluateStatus→PENDING`（`InterviewPersistenceService.java:377`），再发 MQ。

### 2.2 投递失败兜底（生产者端）

`EvaluateStreamProducer.onSendFailed`（`EvaluateStreamProducer.java:68`）：XADD 失败 → 指数退避重试 3 次（100/200/400ms）→ 仍失败则**保持 PENDING**（状态已在 ① 写好），交由补偿任务重投。评估对时效不敏感（用户已交卷），短暂重试即可恢复。

### 2.3 最终一致兜底（补偿任务）

`EvaluateCompensationJob`（每 60s，`EvaluateCompensationJob.java:57`）：

| 场景 | 触发条件 | 处理 |
|------|----------|------|
| **投递静默丢失** | `evaluateStatus=PENDING` 且交卷超过 5min | 重新投递 `sendEvaluateTask` |
| **执行者崩溃卡死** | `evaluateStatus=PROCESSING` 且租约（`evaluateStartedAt`）超过 30min | `resetStaleProcessing` 双检锁复核 → 重置 PENDING → 重新投递 |

`resetStaleProcessing`（`InterviewPersistenceService.java:439`）用**双检锁**：先无锁扫描，再 `SELECT FOR UPDATE` 加锁复核租约是否真过期，避免误伤"只是慢"的消费者。

### 2.4 一致性窗口（诚实说明）

- **窗口 1（MQ 发成功但缓存未删）**：`submitAnswer` 顺序是"DB 提交 → 发 MQ → 删缓存"。若进程在发 MQ 后、删缓存前崩溃，缓存残留旧状态，但 DB 已更新且任务已入队 → 下次读取按 Cache-Aside 重新加载，TTL 兜底，可接受。
- **窗口 2（Redis 与 DB 同时写）**：编排层 `InterviewSessionService` **不加 `@Transactional`**（注释：Redis 不参与 Spring 事务管理器，加了也只能保证 MySQL 原子性），一致性靠"DB 是唯一真相源 + 缓存只做失效"维持。
- **窗口 3（PEL 孤儿消息）**：消费者 `XREADGROUP` 后未 `XACK` 即崩溃，消息滞留 PEL **不被回收**（代码里没有 XAUTOCLAIM/XCLAIM）。实际恢复靠 DB 租约补偿重新 XADD，PEL 孤儿由 `MAXLEN ~1000` 裁剪兜底。

---

## 三、幂等消费

面试模块**没有消息级 dedup 表**（对比简历的 `resume_message_dedup`），幂等完全靠 **DB 行状态 + 悲观锁 CAS**：

1. **`tryClaimForEvaluation`（核心，`InterviewPersistenceService.java:402`）**：`SELECT ... FOR UPDATE` 把 `PENDING→PROCESSING` 原子推进，仅推进成功的调用方才能执行 LLM。
   - 状态为 `EVALUATED / COMPLETED / PROCESSING` → 返回 false → `tryAcquire` 返回 false → 父类**按成功 ACK 跳过**（`AbstractStreamConsumer.java:180`）。
   - `FOR UPDATE` 串行化：并发消费同一 session 时第二个线程阻塞到第一个事务提交后读到 PROCESSING → 跳过，**不会重复调 LLM**。
2. **业务层二次校验**（`EvaluateStreamConsumer.java:251`）：`session.getStatus() == EVALUATED` → 跳过（防报告覆盖）。
3. **答案 upsert 唯一约束**（`upsertAnswer`，`InterviewPersistenceService.java:797`）：先查后插，插入撞 `(session_id, question_index)` 唯一约束 → 捕获 `DataIntegrityViolationException` 退化为更新。`tryInsertAnswer` 用 **REQUIRES_NEW 独立事务**，失败不影响外层事务上下文（必须经 `self` 代理调用才生效）。

**幂等语义**：重复消息（重试重投 / 补偿重投 / 重复消费）最多触发一次 `tryClaim` 成功，其余被状态守卫吸收 → **不重复调 LLM、不覆盖报告、不丢消息**。

---

## 四、顺序消费

- **默认单消费者**：`consumerCount()` 默认 1（`EvaluateStreamConsumer.java:143`，`app.async.consumers.evaluate` 可调）→ 单线程顺序消费。
- **注意**：`consumerCount > 1` 后，Redis 消费者组只保证"每条消息投给一个消费者"，**不保证组内全局顺序**（`AbstractStreamConsumer.java:311` 明确警告）。
- **重试会破坏"入队顺序"**：`retryMessage` 是把失败消息作为**新消息 XADD 到 Stream 尾部**（带 `dueAt` 退避），不是原地重投。因此同一 session 的重试消息会排到比它"更晚"的消息后面。
- **为什么顺序破坏不影响正确性**：因为所有并发/乱序被 **DB CAS（tryClaim）+ EVALUATED 状态守卫**吸收——同一 session 的多个任务无论以什么顺序到达，最终只有一个能 claim 成功，最终状态一定是 EVALUATED。**顺序一致性让位于最终一致性 + 状态机**，这是与简历模块（Kafka 单分区强顺序）最根本的差异。

> 结论：面试评估是"幂等可重放"任务，**不依赖消息顺序**；正确性由 DB 状态机保证，而非 MQ 顺序。

---

## 五、Redis 缓存一致性（Cache-Aside 标准实现）

`InterviewSessionCache` + `InterviewSessionService`（`InterviewSessionService.java:53` 的注释完整阐述了策略）：

- **读路径**：Redis 命中返回；未命中 → 查 MySQL → **回填 Redis**（`restoreSessionFromEntity`）。每次命中刷新 TTL（滑动过期 24h）。
- **写路径（先 DB 后删缓存）**：所有写操作 `persistenceService.xxx()`（MySQL 事务）成功后才 `sessionCache.deleteSession()`（写失效，**不是更新缓存**）。
  - *为什么删而不是更新*：删除后下一次读必然从 MySQL 重载，不存在"DB 已更新、缓存仍旧值"的中间态。
- **删除失败容忍**：Redis 删除失败仅打日志不抛异常（`InterviewSessionService.java:439`），残留旧值在 TTL 内被丢弃，MySQL 始终是唯一真相源。
- **原子性**：`saveAnswerDraft`、`submitAnswer`、`completeInterview` 各自在**一个独立事务内**完成多次 DB 写入，消除"Redis 先写成功、MySQL 后写失败"的部分提交问题。

### 缓存经典问题暴露面

| 问题 | 面试模块现状 | 风险评估 |
|------|--------------|----------|
| **穿透** | sessionId 由服务端生成（16 位 hex，64bit），无法预知 | 低；无空值缓存但难以构造有效 key |
| **击穿** | `getOrRestoreSession` 缓存回填**无互斥锁**，热点 session 过期后并发 miss 会同时打 MySQL | **中**；可用分布式锁或单飞（single-flight）缓解 |
| **雪崩** | 所有 key TTL 相同（24h）但有滑动刷新，访问即续期，不会同步过期 | 低 |
| **不一致** | 先 DB 后删缓存 + TTL 兜底 | 低；删除失败时间窗极短 |

---

## 六、数据库并发与事务设计要点

- **悲观锁贯穿全程**：`updateSessionStatus / updateEvaluateStatus / submitAnswer / saveAnswerDraft / completeInterview / tryClaimForEvaluation / resetStaleProcessing / saveReport / deleteSessionBySessionId` 全部用 `findBySessionIdForUpdate`（`SELECT ... FOR UPDATE`）串行化同一 session 行的并发修改，防 Lost Update 与 TOCTOU。
- **REQUIRES_NEW 隔离**：凡触发 MQ/异步的写方法（`submitAnswer / completeInterview / tryClaimForEvaluation / resetStaleProcessing`）都用独立事务，保证状态变更独立于无事务的编排层立即提交。
- **同类自调用**：`self` 自注入（`@Lazy @Autowired`）绕开 AOP 代理失效问题，`upsertAnswer → tryInsertAnswer` 的 REQUIRES_NEW 才能生效。
- **批量写**：`saveReport` 答案用 `saveAll` 一次性提交，避免循环 save。
- **JPQL 原子删除**：`deleteSessionsByResumeId` 先删答案再删会话，消除先查后删 TOCTOU 与 N+1。

---

## 七、与简历模块（Kafka + Outbox）的方案对比

| 维度 | 简历模块（Kafka） | 面试模块（Redis Stream） |
|------|-------------------|--------------------------|
| **MQ 类型** | Kafka，1 分区，key=resumeId | Redis Stream + 消费者组 |
| **DB↔MQ 一致性** | 事务性 Outbox（业务表+outbox 表同事务，中继扫描投递） | **状态先行 + 补偿任务**（evaluateStatus 权威，无消息表） |
| **投递失败兜底** | Outbox 中继重投 + 死信 | 生产者退避重试 + 补偿任务 5min 重投 |
| **崩溃恢复** | 中继重投 + 幂等去重 | **租约机制**（evaluateStartedAt + 30min 双检锁重置） |
| **幂等** | 消息级 dedup 表 + 唯一约束 + 认领锁 | 行状态 CAS（FOR UPDATE PENDING→PROCESSING） |
| **顺序** | 单分区强顺序（逐简历有序） | 单消费者顺序，重试乱序但被状态机吸收 |
| **消息体** | 携带完整简历文本（PII 风险） | 仅 sessionId（轻量，PII 最小化） |
| **补偿扫描索引** | Outbox `(status,id)` 有索引 | `findByEvaluateStatusAndCompletedAtBefore` **无索引** |

---

## 八、已知边界与改进建议（按优先级）

1. **补偿任务全表扫描无索引**：`findByEvaluateStatusAndCompletedAtBefore` 和 `findStaleProcessing`（`InterviewSessionRepository.java:185,198`）每 60s 扫 `interview_sessions`，实体上**没有 `(evaluate_status, completed_at)` / `(evaluate_status, evaluate_started_at)` 索引** → 数据量大时是全表扫描，建议补索引。
2. **PEL 孤儿消息无回收**：无 XAUTOCLAIM/XCLAIM，崩溃残留的未 ACK 消息只靠 `MAXLEN ~1000` 裁剪。可加 XAUTOCLAIM + 幂等消费来主动回收（并解决长时间未消费的堆积）。
3. **缓存击穿**：`getOrRestoreSession` 回填无锁，热点会话并发 miss 会同时打 MySQL。可加分布式锁 / 单飞。
4. **无鉴权**：sessionId 即访问凭证，任何拿到 sessionId 的人可读答案/生成报告（授权靠不可预测性）。需接入认证授权。
5. **跨用户数据泄露风险**：`getHistoricalQuestions` 无 resumeId 分支按 `skillId` 查全局最近 10 次面试（`InterviewPersistenceService.java:719` 注释自述"若系统有多用户隔离需求，此处存在数据泄露风险"）。
6. **LLM 提示注入**：用户答案/简历文本为不可信输入，直接进入评估 prompt（`AnswerEvaluationService` → `UnifiedEvaluationService`）。项目已有 `common/ai/PromptSanitizer` 基础设施，应确认评估链路是否已应用。
7. **限流覆盖不全**：`submitAnswer` 有全局 10 次限流、`createSession` 全局 5 次；`getSession/getReport/complete` 无限流，可叠加接口防刷。

---

## 九、权衡总结

- **一致性 vs 复杂度**：面试模块用"DB 状态 + 补偿任务 + 租约"取代 Outbox 消息表，省去了消息表与中继，代价是补偿任务依赖全表扫描、恢复延迟取决于 60s 扫描周期与 5min/30min 阈值。
- **顺序 vs 正确性**：放弃 MQ 严格顺序，靠 DB CAS 状态机吸收乱序——前提是任务**幂等可重放**；若未来出现不可重放的有序任务（如累计评分），需改用 Kafka 分区 + key。
- **缓存**：Cache-Aside + 写失效 + TTL 兜底，把一致性风险收敛到"删除失败的时间窗"，用少量短暂脏读换取高性能；代价是每次写多一次 Redis 删除。
- **安全**：消息体轻量（仅 sessionId）是优点；但鉴权、索引、缓存击穿、提示注入仍是需补齐的短板。

> 一句话总结：面试模块走的是"**DB 状态机 + 租约补偿**"路线——MQ 只负责异步通知，正确性由 `evaluateStatus` 状态机 + `FOR UPDATE` CAS + 补偿任务保证；相比简历模块的 Outbox 更轻，但把兜底成本（扫描/租约/恢复延迟）转移到了数据库与调度任务上。
