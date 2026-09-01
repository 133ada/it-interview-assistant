package interview.guide.common.aspect;

import interview.guide.common.annotation.RateLimit;
import interview.guide.common.exception.RateLimitExceededException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * 限流 AOP 切面
 * <p>
 * 核心职责：拦截所有标注了 @RateLimit 的方法，
 * 在方法执行前逐条检查限流规则，任一规则触发则拒绝请求。
 * <p>
 * 工作链路：
 * HTTP请求 → Spring代理拦截 → around() → Lua脚本(Redis) → 放行/拒绝
 */
@Slf4j
@Aspect        // 声明这是一个切面类，Spring AOP 会扫描并激活它
@Component     // 注册为 Spring Bean，使其生效
@RequiredArgsConstructor  // Lombok：自动生成包含 final 字段的构造函数（用于依赖注入）
public class RateLimitAspect {

    // Redisson 是 Redis 的 Java 客户端，提供比 Jedis 更丰富的高级功能
    private final RedissonClient redissonClient;

    // Lua 脚本的原始文本（类加载时静态读取一次，存在内存中）
    private static final String LUA_SCRIPT;

    // Lua 脚本加载到 Redis 后的 SHA1 哈希值
    // Redis 可以用 SHA1 代替完整脚本来调用，减少网络传输量
    private String luaScriptSha;

    // Redisson 提供的脚本执行器，封装了 Redis EVALSHA 命令
    private RScript rScript;

    /**
     * 静态初始化块：类第一次被加载时执行，且只执行一次。
     *
     * 用 static{} 而不是构造函数，是为了让 LUA_SCRIPT 成为 static final 常量——
     * 这样所有实例共享同一份脚本文本，不会重复读取文件。
     *
     * ClassPathResource 会从 resources/ 目录下查找文件，
     * 对应路径为 src/main/resources/scripts/rate_limit_single.lua
     */
    static {
        try {
            ClassPathResource resource = new ClassPathResource("scripts/rate_limit_single.lua");
            LUA_SCRIPT = new String(resource.getContentAsByteArray(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            // 脚本文件加载失败是启动时的致命错误，直接抛出让应用无法启动
            throw new IllegalStateException("加载限流 Lua 脚本失败", e);
        }
    }

    /**
     * @PostConstruct：Bean 创建完成、所有依赖注入完毕后，自动调用这个方法。
     * <p>
     * 相当于"构造函数之后的初始化"，此时 redissonClient 已经注入，可以安全使用。
     * 如果放在构造函数里，redissonClient 还是 null，会空指针。
     */
    @jakarta.annotation.PostConstruct
    public void init() {
        // StringCodec 指定用字符串编解码（Redis 存取的都是字符串，而不是 Java 序列化格式）
        rScript = redissonClient.getScript(StringCodec.INSTANCE);
        loadScript();
    }

    /**
     * 把 Lua 脚本上传到 Redis，Redis 返回该脚本的 SHA1 哈希。
     * <p>
     * SCRIPT LOAD 命令：将脚本缓存在 Redis 服务端，后续用 EVALSHA + SHA1 调用，
     * 避免每次都传输完整脚本文本（网络优化）。
     * <p>
     * 注意：Redis 重启后脚本缓存会丢失，所以后面调用失败时需要重新 load（见 NOSCRIPT 处理）。
     */
    private void loadScript() {
        this.luaScriptSha = rScript.scriptLoad(LUA_SCRIPT);
        log.info("限流 Lua 脚本加载完成, SHA1: {}", luaScriptSha);
    }

    /**
     * 核心方法：环绕通知（Around Advice）。
     *
     * @Around 的切点表达式解释：
     * @annotation(xxx.RateLimit) → 方法上直接标了单个 @RateLimit
     * @annotation(xxx.RateLimit.Container) → 方法上标了多个 @RateLimit（Repeatable 容器）
     * || 表示两种情况都要拦截
     * <p>
     * ProceedingJoinPoint 代表"被拦截的方法调用"，
     * 调用 joinPoint.proceed() 才会真正执行原始方法。
     * 不调用 proceed() = 方法被完全拦截，不会执行。
     */
    @Around("@annotation(interview.guide.common.annotation.RateLimit) || " +
            "@annotation(interview.guide.common.annotation.RateLimit.Container)")
    public Object around(ProceedingJoinPoint joinPoint) throws Throwable {

        // 调用 GET /api/llm-provider/list 来真是模拟下述方法

        // MethodSignature 包含方法的完整元信息（名称、参数、注解等）
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod(); //

        // 用于生成 Redis Key 的一部分，确保不同类/方法的限流互不干扰
        String className = method.getDeclaringClass().getSimpleName();  // "LlmProviderController"
        String methodName = method.getName();                            // "listProviders"

        // getAnnotationsByType 支持 @Repeatable：
        // 不管方法上有 1 个还是多个 @RateLimit，都统一返回数组
        RateLimit[] rules = method.getAnnotationsByType(RateLimit.class); //[@RateLimit(dimension=GLOBAL, count=30, interval=1, timeUnit=SECONDS)]

        // 所有规则共用同一个时间戳和 requestId，保证"这次请求"的一致性
        long nowMs = System.currentTimeMillis();  // 1718000000500L;
        // UUID 作为本次请求的唯一标识，写入 Redis Sorted Set，用于后续令牌回收
        String requestId = UUID.randomUUID().toString(); // "a1b2c3d4-...";

        // 逐条执行限流规则，任一失败立即拒绝，不再检查后续规则（短路）
        for (RateLimit rule : rules) {
            // 将注解上的 interval + timeUnit 转换为毫秒
            // 例如：interval=1, timeUnit=SECONDS → 1000ms
            long intervalMs = calculateIntervalMs(rule.interval(), rule.timeUnit()); // 1000L;

            // 生成 Redis Key，不同维度（GLOBAL/IP/USER）的 Key 不同
            String key = generateKey(className, methodName, rule.dimension()); // "ratelimit:{LlmProviderController:listProviders}:global"

            // 调用 Lua 脚本，返回 1（通过）或 0（被限流）
            Long result = executeRateLimitScript(key, nowMs, requestId, intervalMs, rule.count());

            if (result == null || result == 0) {
                // 触发限流：执行降级逻辑或抛出异常
                return handleRateLimitExceeded(joinPoint, rule, key);
            }
        }

        // 所有规则全部通过，放行——真正执行被拦截的 Controller 方法
        return joinPoint.proceed();
    }

    /**
     * 执行 Lua 脚本的方法。
     * <p>
     * 为什么不直接 EVAL 而是 EVALSHA？
     * EVAL 每次都要把完整脚本文本发给 Redis，EVALSHA 只发 40 字节的哈希，更高效。
     *
     * @param key        Redis 限流维度键（不含 :value / :permits 后缀，Lua 内部拼接）
     * @param nowMs      当前时间戳（毫秒）
     * @param requestId  请求唯一标识，写入 ZSet 成员
     * @param intervalMs 时间窗口（毫秒）
     * @param count      窗口内最大令牌数
     */
    private Long executeRateLimitScript(String key, long nowMs, String requestId,
                                        long intervalMs, double count) {
        // Redis KEYS 参数：Lua 脚本通过 KEYS[1] 访问
        List<Object> keysList = Collections.singletonList(key); // ["ratelimit:{LlmProviderController:listProviders}:global"]

        // Redis ARGV 参数：对应 Lua 脚本中的 ARGV[1]~ARGV[5]
        // 注意：String.valueOf(1) 写死为 1，表示每次消耗 1 个令牌
        Object[] args = {
                String.valueOf(nowMs),      // ARGV[1]: 当前时间戳
                String.valueOf(1),        // ARGV[2]: 申请令牌数（固定为1）
                String.valueOf(intervalMs), // ARGV[3]: 时间窗口（毫秒）
                String.valueOf(count),      // ARGV[4]: 最大令牌数
                requestId                   // ARGV[5]: 请求唯一标识
        };

        // Object[] args = {
        //    "1718000000500",    // ARGV[1]: nowMs
        //    "1",                // ARGV[2]: permits（固定消耗1个令牌）
        //    "1000",             // ARGV[3]: intervalMs（1秒窗口）
        //    "30.0",             // ARGV[4]: max_tokens（每秒最多30次）
        //    "a1b2c3d4-..."      // ARGV[5]: requestId
        //}
        try {
            Object resultObj = rScript.evalSha(
                    RScript.Mode.READ_WRITE,        // 脚本会读写数据，不能用只读模式
                    luaScriptSha,                   // 脚本的 SHA1，代替完整脚本文本
                    RScript.ReturnType.VALUE,        // 期望 Lua 返回单个值（0 或 1）
                    keysList,
                    args
            );
            return convertToLong(resultObj);

        } catch (org.redisson.client.RedisException e) {
            // NOSCRIPT 错误：Redis 重启后脚本缓存丢失，需要重新上传
            // 重新 loadScript() 后立刻重试一次
            if (e.getMessage() != null && e.getMessage().contains("NOSCRIPT")) {
                loadScript();
                Object resultObj = rScript.evalSha(
                        RScript.Mode.READ_WRITE,
                        luaScriptSha,
                        RScript.ReturnType.VALUE,
                        keysList,
                        args
                );
                return convertToLong(resultObj);
            }
            // 其他 Redis 异常（连接断开、超时等）向上抛出
            throw e;
        }
    }

    /**
     * 将注解上的 interval + timeUnit 转换为毫秒。
     * <p>
     * 使用 switch 表达式（Java 14+），比传统 switch 更简洁。
     * 例如：interval=2, MINUTES → 2 * 60 * 1000 = 120000ms
     */
    private long calculateIntervalMs(long interval, RateLimit.TimeUnit unit) {
        return switch (unit) {
            case MILLISECONDS -> interval;
            case SECONDS -> interval * 1000;
            case MINUTES -> interval * 60 * 1000;
            case HOURS -> interval * 3600 * 1000;
            case DAYS -> interval * 86400 * 1000;
        };
    }

    /**
     * 将 Redis 返回值转换为 Long。
     * <p>
     * 之所以需要这个转换，是因为：
     * - Redisson 有时返回 Long，有时返回 Integer（取决于底层解码），
     * - 统一用 instanceof Number 兜底，再转 longValue()，避免 ClassCastException。
     * - String 分支是额外防御，理论上不会触发，但防止极端情况。
     */
    private Long convertToLong(Object obj) {
        if (obj instanceof Number n) {
            return n.longValue();  // Long、Integer、Short 都是 Number 的子类
        }
        if (obj instanceof String s) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException e) {
                log.warn("无法将字符串转换为Long: {}", obj);
                return null;
            }
        }
        log.warn("不支持的对象类型转换为Long: {}", obj != null ? obj.getClass().getName() : "null");
        return null;
    }

    /**
     * 生成 Redis Key，格式为：
     * ratelimit:{ClassName:methodName}:维度[:附加标识]
     * <p>
     * 花括号 {} 是 Redis Cluster 的 Hash Tag：
     * 同一个 {} 内容的 key 会被分配到同一个哈希槽（slot）。
     * 这个切面会对同一个方法生成 key:value 和 key:permits 两个 Redis key，
     * 它们必须在同一个 slot 上，Lua 脚本才能原子地操作两者。
     * <p>
     * 示例：
     * GLOBAL → "ratelimit:{LlmProviderController:listProviders}:global"
     * IP     → "ratelimit:{LlmProviderController:listProviders}:ip:192.168.1.1"
     * USER   → "ratelimit:{LlmProviderController:listProviders}:user:user123"
     */
    private String generateKey(String className, String methodName, RateLimit.Dimension dimension) {
        String hashTag = "{" + className + ":" + methodName + "}";
        String keyPrefix = "ratelimit:" + hashTag;

        return switch (dimension) {
            case GLOBAL -> keyPrefix + ":global";
            case IP -> keyPrefix + ":ip:" + getClientIp();     // 按来源 IP 独立限流
            case USER -> keyPrefix + ":user:" + getCurrentUserId(); // 按用户 ID 独立限流
        };
    }

    /**
     * 限流触发后的处理逻辑，支持两种方式：
     * 1. 降级（fallback）：调用同类中指定的降级方法，返回兜底数据
     * 2. 异常：抛出 RateLimitExceededException，由全局异常处理器返回 429 响应
     */
    private Object handleRateLimitExceeded(ProceedingJoinPoint joinPoint,
                                           RateLimit rateLimit, String key) throws Throwable {
        String methodName = joinPoint.getSignature().getName();

        // 如果注解上配置了 fallback 方法名，尝试调用降级方法
        if (rateLimit.fallback() != null && !rateLimit.fallback().isEmpty()) {
            try {
                Method fallbackMethod = findFallbackMethod(joinPoint, rateLimit.fallback());
                if (fallbackMethod != null) {
                    log.debug("限流触发，执行降级方法: {}.{} -> {}",
                            joinPoint.getTarget().getClass().getSimpleName(),
                            methodName,
                            rateLimit.fallback());

                    // 降级方法支持两种签名：
                    //   有参：参数列表与原方法一致（可以拿到原始入参）
                    //   无参：不需要原始入参
                    if (fallbackMethod.getParameterCount() > 0) {
                        return fallbackMethod.invoke(joinPoint.getTarget(), joinPoint.getArgs());
                    } else {
                        return fallbackMethod.invoke(joinPoint.getTarget());
                    }
                }
            } catch (Exception e) {
                // 降级方法本身也失败了，记录日志后继续走抛异常的路径
                log.error("降级方法执行失败: {}", rateLimit.fallback(), e);
            }
        }

        // 没有配置 fallback，或 fallback 执行失败，直接抛出限流异常
        log.debug("限流触发，拒绝请求: key={}, count={} per {} {}",
                key, rateLimit.count(), rateLimit.interval(), rateLimit.timeUnit());
        throw new RateLimitExceededException("请求过于频繁，请稍后再试");
    }

    /**
     * 通过反射在目标类中查找降级方法。
     * <p>
     * 查找策略（优先级从高到低）：
     * 1. 参数列表与原方法完全一致的同名方法
     * 2. 无参的同名方法
     * <p>
     * getDeclaredMethod 只查找当前类声明的方法（不含继承），
     * setAccessible(true) 允许调用 private 方法。
     */
    private Method findFallbackMethod(ProceedingJoinPoint joinPoint, String fallbackName) {
        Class<?> targetClass = joinPoint.getTarget().getClass();
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Class<?>[] parameterTypes = signature.getParameterTypes();  // 原方法的参数类型数组

        try {
            // 优先找参数一致的方法
            Method method = targetClass.getDeclaredMethod(fallbackName, parameterTypes);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException e) {
            try {
                // 退而求其次，找无参方法
                Method method = targetClass.getDeclaredMethod(fallbackName);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ex) {
                log.warn("未找到降级方法: {}.{} (需无参或参数列表一致)",
                        targetClass.getSimpleName(), fallbackName);
                return null;
            }
        }
    }

    /**
     * 获取客户端真实 IP。
     * <p>
     * 直接取 remoteAddr 在有反向代理（Nginx、CDN）时会拿到代理服务器的 IP，
     * 所以要依次检查以下请求头，它们由代理服务器填写，记录的是原始客户端 IP：
     * <p>
     * X-Forwarded-For   → 标准格式，可能包含多个 IP（"client, proxy1, proxy2"），取第一个
     * X-Real-IP         → Nginx 常用，单个 IP
     * Proxy-Client-IP   → Apache 代理常用
     * WL-Proxy-Client-IP→ WebLogic 代理常用
     * <p>
     * 最后兜底才用 remoteAddr（直连时有效）。
     */
    private String getClientIp() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return "unknown";  // 非 Web 环境（如定时任务中调用），无法获取 IP
        }

        HttpServletRequest request = attributes.getRequest();

        // 按优先级依次尝试各个请求头
        String ip = request.getHeader("X-Forwarded-For");

        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();  // 直连时的兜底
        }

        // X-Forwarded-For 可能是 "client, proxy1, proxy2" 格式，取第一个才是原始客户端
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }

        return ip != null ? ip : "unknown";
    }

    /**
     * 获取当前登录用户 ID。
     * <p>
     * 用于 dimension = USER 时生成独立的限流 Key（不同用户互不影响）。
     * <p>
     * 获取来源（按优先级）：
     * 1. request.getAttribute("userId")：通常由 JWT 认证过滤器解析后存入
     * 2. 请求头 X-User-Id：某些网关架构会直接在 Header 中传递
     * 3. "anonymous"：未登录用户，所有匿名请求共享同一个限流桶
     * <p>
     * 注意：匿名用户共用 "anonymous" key，意味着所有未登录用户共享限流配额，
     * 而不是每个匿名访客独立限流。如需按 IP 限流匿名用户，应使用 Dimension.IP。
     */
    private String getCurrentUserId() {
        ServletRequestAttributes attributes =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attributes == null) {
            return "anonymous";
        }

        HttpServletRequest request = attributes.getRequest();

        // 优先从 request Attribute 获取（通常是认证中间件解析 Token 后存入的）
        Object userId = request.getAttribute("userId");
        if (userId != null) {
            return userId.toString();
        }

        // 其次从请求头获取（网关层透传的场景）
        userId = request.getHeader("X-User-Id");
        if (userId != null) {
            return userId.toString();
        }

        return "anonymous";
    }
}