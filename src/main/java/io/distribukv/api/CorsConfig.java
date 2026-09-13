package io.distribukv.api;

import io.distribukv.config.ClusterProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/** Lets the static project page read cluster status (read-only) from the live demo. */
@Configuration
public class CorsConfig implements WebMvcConfigurer {

    private final String[] allowedOrigins;

    public CorsConfig(ClusterProperties props) {
        List<String> origins = props.publicDemo().allowedOrigins();
        this.allowedOrigins = origins == null ? new String[0] : origins.toArray(String[]::new);
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        if (allowedOrigins.length == 0) {
            return;
        }
        registry.addMapping("/cluster/status").allowedOrigins(allowedOrigins).allowedMethods("GET");
        registry.addMapping("/cluster/ring").allowedOrigins(allowedOrigins).allowedMethods("GET");
    }
}
