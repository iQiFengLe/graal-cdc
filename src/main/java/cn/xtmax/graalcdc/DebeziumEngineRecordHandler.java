package cn.xtmax.graalcdc;

import cn.xtmax.graalcdc.config.ListenDatabaseInstanceConfig;
import cn.xtmax.graalcdc.config.SystemConfig;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.debezium.engine.ChangeEvent;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.InvalidMarshallableException;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.ReadMarshallable;
import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * debezium 引擎记录处理器
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/29 18:18
 */
@Slf4j
public class DebeziumEngineRecordHandler {

    final SystemConfig systemConfig;

    final ScriptScheduler scriptScheduler;

    final ScriptExecutor scriptExecutor;

    final SingleChronicleQueue queue;

    final ExcerptAppender queueExcerptAppender;

    final Set<String> databases = ConcurrentHashMap.newKeySet();
    final Set<String> tables = ConcurrentHashMap.newKeySet();

    /**
     * 脚本处理器
     *
     * @param filepath  脚本文件路径
     * @param databases 数据库名称
     * @param tables    表名称
     * @param thread    线程
     * @param semaphore 信号量
     * @param future    任务
     * @param tailerReader 读取器
     */
    record ScriptHandler(String filepath,
                         Set<String> databases,
                         Set<String> tables,
                         Thread thread,
                         Semaphore semaphore,
                         CompletableFuture<Void> future,
                         ExcerptTailer excerptTailer,
                         TailerReader tailerReader) {
    }

    private final Map<String, ScriptHandler> scriptHandlerMap = new ConcurrentHashMap<>();

    private final ListenDatabaseInstanceConfig databaseInstanceConfig;

    public DebeziumEngineRecordHandler(SystemConfig systemConfig,
                                       ScriptScheduler scriptScheduler,
                                       ScriptExecutor scriptExecutor,
                                       ListenDatabaseInstanceConfig databaseInstanceConfig) {
        this.systemConfig = systemConfig;
        this.scriptScheduler = scriptScheduler;
        this.scriptExecutor = scriptExecutor;
        this.databaseInstanceConfig = databaseInstanceConfig;
        if (databaseInstanceConfig.getDatabases() != null){
            databases.addAll(databaseInstanceConfig.getDatabases());
        }
        if (databaseInstanceConfig.getTables() != null){
            tables.addAll(databaseInstanceConfig.getTables());
        }
        this.queue = SingleChronicleQueueBuilder
            .binary(systemConfig.getQueueDirectory())
            .rollCycle(RollCycles.FAST_HOURLY)
            .build();
        this.queueExcerptAppender = queue.createAppender();
    }


    final class TailerReader implements ReadMarshallable {
        final StringBuilder sb = new StringBuilder(2 * 1024);
        final String scriptId;

        TailerReader(String scriptId) {
            this.scriptId = scriptId;
        }
        @Override
        public void readMarshallable(WireIn wire) throws IORuntimeException, InvalidMarshallableException {
            ValueIn valueIn = wire.getValueIn();
            valueIn.text(sb);
            if (sb.isEmpty()) {
                return;
            }
            String text = sb.toString();
            CompletableFuture<?> future = scriptScheduler.schedule(() -> scriptExecutor.execute(scriptId, text));
            future.handle((_, _e) -> {
                if (_e != null){
                    // 记录下错误
                    log.error("scriptId: {}, 执行脚本错误！", scriptId, _e);
                }
                return null;
            })
            // 等待执行完成，不然ExcerptTailer对应的position会前移
            .join();
        }
    }

    ScriptHandler createScriptHandler(String scriptId, Set<String> databases, Set<String> tables) {
        Semaphore semaphore = new Semaphore(1);
        CompletableFuture<Void> future = new CompletableFuture<>();
        TailerReader tailerReader = new TailerReader(scriptId);
        ExcerptTailer tailer = queue.createTailer(scriptId);
        Thread thread = Thread.startVirtualThread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    semaphore.acquire();
                    tailer.readDocument(tailerReader);
                }
                future.complete(null);
            } catch (Throwable e){
                // 释放资源
                try{
                    tailer.close();
                }catch (Throwable t){
                    log.error("关闭 Tailer 错误！", t);
                }
                future.completeExceptionally(e);
            }
        });
        return new ScriptHandler(scriptId, databases, tables, thread, semaphore, future, tailer, tailerReader);
    }

    public void setScriptHandler(String scriptId, Set<String> databases, Set<String> tables) {
        scriptHandlerMap.computeIfAbsent(scriptId, _ -> createScriptHandler(scriptId, databases, tables));
    }

    public void handle(ChangeEvent<String, String> record) {
        String value = record.value();
        // 墓碑消息
        if (value == null) {
            return;
        }
        JSONObject payloadBody = JSON.parseObject(value).getJSONObject("payload");
        if (payloadBody == null) {
            return;
        }
        // Schema change / DDL（直接丢弃）
        if (payloadBody.containsKey("ddl")) {
            return;
        }
        String op = payloadBody.getString("op");
        // 只保留行级变更
        if (!"c".equals(op) && !"u".equals(op) && !"d".equals(op) && !"r".equals(op)) {
            return;
        }
        // after 为 null 的也不要（比如某些 c 是 schema）
        if (!payloadBody.containsKey("after")) {
            return;
        }
        JSONObject source = payloadBody.getJSONObject("source");
        String db = source.getString("db");
        String table = source.getString("table");
        // 防止内存队列爆炸，这里快速写入本地队列
        queueExcerptAppender.writeText(value);
        // 释放信号
        scriptHandlerMap.forEach((_, scriptHandler) -> scriptHandler.semaphore.release());
    }
}
