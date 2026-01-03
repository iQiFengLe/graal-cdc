package cn.xtmax.graalcdc;

import cn.xtmax.graalcdc.config.ListenDatabaseInstanceConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * 类说明
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/29 14:43
 */
@Component
public class TestRunner implements ApplicationRunner {

    @Autowired
    DebeziumEngineManager debeziumEngineManager;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        ListenDatabaseInstanceConfig config = new ListenDatabaseInstanceConfig();
        config.setHost("192.168.0.136");
        config.setPort(6801);
        config.setDbType(DbType.MYSQL);
        config.setUsername("root");
        config.setPassword("HongWaDB666.777");
        config.setName("MYSQL");
        config.setServerId(1000);
        debeziumEngineManager.runner(config);

    }
}
