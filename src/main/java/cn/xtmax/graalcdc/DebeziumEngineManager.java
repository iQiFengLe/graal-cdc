package cn.xtmax.graalcdc;

import cn.xtmax.graalcdc.config.ListenDatabaseInstanceConfig;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/27 20:01
 */
@Component
public class DebeziumEngineManager {


    private final Map<String, Item> engines = new ConcurrentHashMap<>();

    record Item(RecordHandler recordHandler, DebeziumEngine<ChangeEvent<String, String>> engine) {
    }

    @Autowired
    ScriptExecutor scriptExecutor;


    private final String offsetFilePath = "./data";

    public static class RecordHandler{
        public void handle(ChangeEvent<String, String> record) {
            String value = record.value();
            if (value == null){
                // 可能是墓碑消息（Tombstone），直接跳过
                return;
            }
            JSONObject payload = JSON.parseObject(value);

            String op = payload.getString("op");


        }
    }

    private Item createEngine(ListenDatabaseInstanceConfig databaseInstanceConfig){
        DbType dbType = databaseInstanceConfig.getDbType();
        Properties props = new Properties();
        /* 关键：保存消费位点，挂了重启全靠它 */
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsetFilePath + "/" + dbType.getValue() + "-" + databaseInstanceConfig.getName() + "_offset.dat");
        // 数据库连接配置
        props.setProperty("database.hostname", databaseInstanceConfig.getHost());
        props.setProperty("database.port", String.valueOf(databaseInstanceConfig.getPort()));
        props.setProperty("database.user", databaseInstanceConfig.getUsername());
        props.setProperty("database.password", databaseInstanceConfig.getPassword());
        props.setProperty("database.server.name", databaseInstanceConfig.getName());
        if (databaseInstanceConfig.getDatabases() != null && !databaseInstanceConfig.getDatabases().isEmpty()){
            // 要监控的数据库
            props.setProperty("database.include.list", String.join(",", databaseInstanceConfig.getDatabases()));
        }
        if (databaseInstanceConfig.getTables() != null && !databaseInstanceConfig.getTables().isEmpty()){
            // 要监控的表
            props.setProperty("table.include.list", String.join(",", databaseInstanceConfig.getTables()));
        }
        switch (dbType){
            case MYSQL:
                props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
                // MySQL 必须配这个，否则启动报错
                props.setProperty("schema.history.internal", "io.debezium.relational.history.FileDatabaseHistory");
                props.setProperty("schema.history.internal.file.filename", offsetFilePath + "/mysql-" + databaseInstanceConfig.getName() + "_dbhistory.dat");
                break;
            case POSTGRESQL:
                props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
                props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
                props.setProperty("database.history.file.filename", offsetFilePath + "/" + databaseInstanceConfig.getName() + "_dbhistory.dat");
                break;
            default:
                throw new UnsupportedOperationException("不支持的数据库类型: " + dbType);
        }

        RecordHandler recordHandler = new RecordHandler();

        return new Item(
            recordHandler,
            DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(recordHandler::handle)
                .build()
        );
    }


    public DebeziumEngine<ChangeEvent<String, String>> get(ListenDatabaseInstanceConfig databaseInstanceConfig){
        Objects.requireNonNull(databaseInstanceConfig, "数据库实例不能为 NULL");
        String key = databaseInstanceConfig.uniqueKey();
        return engines.computeIfAbsent(key, k -> createEngine(databaseInstanceConfig)).engine;
    }

}
