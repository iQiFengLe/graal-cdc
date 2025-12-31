package cn.xtmax.graalcdc;

import cn.xtmax.graalcdc.config.ListenDatabaseInstanceConfig;
import cn.xtmax.graalcdc.config.SystemConfig;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/27 20:01
 */
@Component
public class DebeziumEngineManager {


    private final Map<String, Item> engines = new ConcurrentHashMap<>();

    record Item(DebeziumEngine<ChangeEvent<String, String>> engine,
                ExecutorService executorService
    ) {
    }

    @Autowired
    SystemConfig systemConfig;

    @Autowired
    DebeziumEngineRecordHandler recordHandler;

    private Item createEngine(ListenDatabaseInstanceConfig databaseInstanceConfig) {
        DbType dbType = databaseInstanceConfig.getDbType();
        Properties props = new Properties();

        String name = dbType.getValue() + "-" + databaseInstanceConfig.getName();
        String offsetPath = systemConfig.getOffsetDirectory();


        // debezium 引擎配置
        props.setProperty("name", name);
        props.setProperty("topic.prefix", dbType.getValue() + "_" + databaseInstanceConfig.getName());

        // 配置 offset 存储文件
        props.setProperty(
            "offset.storage",
            "org.apache.kafka.connect.storage.FileOffsetBackingStore"
        );
        props.setProperty(
            "offset.storage.file.filename",
            offsetPath + "/" + name + "_offset.dat"
        );

        // 通用数据库连接配置
        props.setProperty("database.hostname", databaseInstanceConfig.getHost());
        props.setProperty("database.port", String.valueOf(databaseInstanceConfig.getPort()));
        props.setProperty("database.user", databaseInstanceConfig.getUsername());
        props.setProperty("database.password", databaseInstanceConfig.getPassword());

        // 禁用 schema
        props.setProperty("schemas.enable", "false");

        if (databaseInstanceConfig.getTables() != null && !databaseInstanceConfig.getTables().isEmpty()) {
            props.setProperty("table.include.list", String.join(",", databaseInstanceConfig.getTables()));
        }

        switch (dbType) {
            case MYSQL -> {
                props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
                int serverId = databaseInstanceConfig.getServerId();
                if (serverId <= 0) {
                    throw new IllegalArgumentException("MySQL server-id 必须在 1 ~ 2^32-1 之间");
                }
                props.setProperty("database.server.id", String.valueOf(serverId));

                if (databaseInstanceConfig.getDatabases() != null
                    && !databaseInstanceConfig.getDatabases().isEmpty()) {
                    props.setProperty(
                        "database.include.list",
                        String.join(",", databaseInstanceConfig.getDatabases())
                    );
                }
                props.setProperty(
                    "schema.history.internal",
                    "io.debezium.storage.file.history.FileSchemaHistory"
                );
                props.setProperty(
                    "schema.history.internal.file.filename",
                    offsetPath + "/mysql-" + databaseInstanceConfig.getName() + "_dbhistory.dat"
                );
            }

            case POSTGRESQL -> {
                props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
                props.setProperty(
                    "database.dbname",
                    databaseInstanceConfig.getDatabases().iterator().next()
                );

                props.setProperty("plugin.name", "pgoutput");
                props.setProperty("slot.name", "debezium_" + databaseInstanceConfig.getName());
                props.setProperty("publication.name", "debezium_" + databaseInstanceConfig.getName());

                // schema history（PG）
                props.setProperty(
                    "database.history",
                    "io.debezium.relational.history.FileDatabaseHistory"
                );
                props.setProperty(
                    "database.history.file.filename",
                    offsetPath + "/pg-" + databaseInstanceConfig.getName() + "_dbhistory.dat"
                );
            }

            default -> throw new UnsupportedOperationException("不支持的数据库类型: " + dbType);
        }

        DebeziumEngine<ChangeEvent<String, String>> engine =
            DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(v -> recordHandler.handle(databaseInstanceConfig, v))
                .build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(engine);
        return new Item(engine, executorService);
    }


    public void runner(ListenDatabaseInstanceConfig databaseInstanceConfig){
        Objects.requireNonNull(databaseInstanceConfig, "数据库实例不能为 NULL");
        String key = databaseInstanceConfig.uniqueKey();
        engines.computeIfAbsent(key, _ -> createEngine(databaseInstanceConfig));
    }

}
