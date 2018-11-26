package io.yorkecao.sparkclientdemo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author Yorke
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "spark-client")
public class SparkClientConfiguration {

    private String appName;
    private String sparkHome;
    private String master;
    private String sparkDriverMemory;
    private String sparkExecutorMemory;
    private String sparkExecutorCores;
    private String appResource;
    private String mainClass;
    private String deployMode;
}
