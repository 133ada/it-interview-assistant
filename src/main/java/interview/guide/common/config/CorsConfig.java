package interview.guide.common.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

import java.util.Arrays;
import java.util.List;

/**
 * CORS跨域配置
 */
@Configuration
@RequiredArgsConstructor
public class CorsConfig {

    private final CorsProperties corsProperties;

    /**
     * 向 Spring 容器中注册一个 CorsFilter（跨域过滤器）
     * 所有的 HTTP 请求在进入 Controller 之前都会先经过这个过滤器来校验跨域权限
     */
    @Bean
    public CorsFilter corsFilter() {
        CorsConfiguration config = new CorsConfiguration(); // 创建一个 CORS 配置对象，用来承载具体的跨域规则（如允许的域名、方法、请求头等）

        // 从配置类中获取允许跨域的原始域名字符串（例如从 yml 中读出 "http://localhost:5173 , http://localhost:5174 , http://localhost:80"）
        String allowedOrigins = corsProperties.getAllowedOrigins();
        Arrays.stream(allowedOrigins.split(",")) //  将逗号分隔的域名字符串拆分成数组并转化为 Stream 流
                .map(String::trim) // 剔除每个域名两端可能存在的空格
                .forEach(config::addAllowedOrigin); // 将清洗后的合法域名逐个添加到 CORS 允许的白名单中

        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS")); // 设置允许的前端 HTTP 请求方法，覆盖了常见的增删改查及布防请求
        config.setAllowedHeaders(List.of("*")); // 设置允许客户端携带的自定义 HTTP 请求头，"*" 代表允许所有的 Header（如 Authorization、Content-Type 等）
        // 允许客户端请求携带凭证（如 Cookie、HTTP 认证信息等）。注意：开启此项时，AllowedOrigin 不能设置为 "*" 盲信，必须是具体的域名白名单
        config.setAllowCredentials(true);
        config.setMaxAge(3600L); // 设置跨域预检请求（OPTIONS 请求）的缓存时间，单位为秒。这里设置为 3600 秒（1小时）。

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource(); // 创建一个基于 URL 路径映射的 CORS 配置源对象
        source.registerCorsConfiguration("/api/**", config); // 将上面配置好的规则（config），绑定到特定的 URL 匹配模式上。此处代表只有以 "/api/" 开头的后端接口才应用这套跨域规则

        return new CorsFilter(source);
    }
}
