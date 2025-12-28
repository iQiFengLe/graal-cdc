package cn.xtmax.graalcdc.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/28 12:52
 */
@Configuration
public class BasicConfig {


    @Bean
    public WebClient webClient(){
        return WebClient.builder().build();
    }
}
