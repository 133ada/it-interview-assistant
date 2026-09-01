package interview.guide.common.ai;

import lombok.extern.slf4j.Slf4j;
import org.springaicommunity.agent.tools.SkillsTool;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 * spring-ai-agent-utils 工具配置。
 * 当前先接入 SkillsTool，复用 resources/skills/{skillId}/SKILL.md。
 */
@Configuration // 表示这是一个 配置类 Spring 启动时会自动加载
@Slf4j // Lombok 提供日志对象：
public class AgentUtilsConfiguration {

    private final ResourceLoader resourceLoader; // Spring 提供的


    // 资源加载器 classpath:xxx 、 file:xxx
    private final AgentUtilsProperties agentUtilsProperties; // 自定义配置类（你项目里的）

    public AgentUtilsConfiguration(
            ResourceLoader resourceLoader, AgentUtilsProperties agentUtilsProperties
    ) {
        this.resourceLoader = resourceLoader;
        this.agentUtilsProperties = agentUtilsProperties;
    }

    @Bean("interviewSkillsToolCallback")
    public ToolCallback interviewSkillsToolCallback() {
        String configuredSkillsRoot = agentUtilsProperties.getSkillsRoot(); // 资源配置目录
        String normalizedSkillsRoot = normalizeSkillsRoot(configuredSkillsRoot); // 目录标准化处理
        Resource skillsRootResource = resourceLoader.getResource(normalizedSkillsRoot); // 得到资源配置目录下的资源文件

        if (!skillsRootResource.exists()) {
            throw new IllegalStateException("未找到 skills 根目录，请检查配置: " + normalizedSkillsRoot);
        }

        log.info("AgentUtils SkillsTool 已启用，skillsRoot={}, configured={}", normalizedSkillsRoot, configuredSkillsRoot);

        return SkillsTool.builder()
                .addSkillsResource(skillsRootResource)
                .build();
    }

    private String normalizeSkillsRoot(String raw) {
        if (raw == null || raw.isBlank()) {
            return "classpath:skills";
        }

        String normalized = raw.trim();
        normalized = normalized.replace('\\', '/');

        if (normalized.endsWith("/SKILL.md")) {
            normalized = normalized.substring(0, normalized.length() - "/SKILL.md".length());
        }

        int wildcardIndex = normalized.indexOf('*');
        if (wildcardIndex >= 0) {
            normalized = normalized.substring(0, wildcardIndex);
        }

        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        return normalized.isBlank() ? "classpath:skills" : normalized;
    }
}
