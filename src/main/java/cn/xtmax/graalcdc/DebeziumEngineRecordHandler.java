package cn.xtmax.graalcdc;

import cn.xtmax.graalcdc.config.ListenDatabaseInstanceConfig;
import cn.xtmax.graalcdc.config.SystemConfig;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import io.debezium.engine.ChangeEvent;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.MarshallableIn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/**
 * debezium 引擎记录处理器
 *
 * @author 起风了
 * @mail m.zxt@foxmail.com
 * @date 2025/12/29 18:18
 */
@Slf4j
@Component
public class DebeziumEngineRecordHandler {

    @Autowired
    SystemConfig systemConfig;

    @Autowired
    private ScriptScheduler scriptScheduler;
    @Autowired
    private ScriptExecutor scriptExecutor;


    final static class QueueObject {
        final SingleChronicleQueue queue;
        final ExcerptAppender excerptAppender;

        final Map<Thread, Map<String, ExcerptTailer>> tailerMap = new ConcurrentHashMap<>();

        QueueObject(SingleChronicleQueue queue, ExcerptAppender excerptAppender) {
            this.queue = queue;
            this.excerptAppender = excerptAppender;
        }

        ExcerptTailer getTailer(String scriptId) {
            return getTailer(scriptId, Thread.currentThread());
        }

        ExcerptTailer getTailer(String scriptId, Thread thread) {
            return tailerMap.computeIfAbsent(thread, _ -> new ConcurrentHashMap<>())
                .computeIfAbsent(scriptId, _ -> queue.createTailer(scriptId));
        }

    }
    private final Map<ListenDatabaseInstanceConfig, QueueObject> queueMap = new ConcurrentHashMap<>();


    record ScriptHandler(String filepath, Set<String> databases, Set<String> tables, Thread thread,
                         CompletableFuture<Void> future) {
    }


    private final Map<String, ScriptHandler> scriptFilterMap = new ConcurrentHashMap<>();


    public void setScriptHandler(String scriptId, Set<String> names, Set<String> databases, Set<String> tables) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread thread = Thread.startVirtualThread(() -> {
            // 初始化
            Set<Map.Entry<ListenDatabaseInstanceConfig, QueueObject>> entries = queueMap.entrySet();
            ArrayList<ExcerptTailer> tailers = new ArrayList<>();
            for (Map.Entry<ListenDatabaseInstanceConfig, QueueObject> entry : entries) {
                ListenDatabaseInstanceConfig databaseInstanceConfig = entry.getKey();
                QueueObject queueObject = entry.getValue();
                if (databases != null && !databases.contains(databaseInstanceConfig.getName())) {
                    continue;
                }
                tailers.add(queueObject.getTailer(scriptId));
            }
            Pauser pauser = Pauser.balanced();
            try {
                while (!Thread.currentThread().isInterrupted()) {

                    List<String> texts = tailers.stream()
                        .map(MarshallableIn::readText).filter(Objects::nonNull)
                        .toList();
                    if (texts.isEmpty()) {
                        pauser.pause();
                    } else {
                        pauser.reset();
                        for (String text : texts) {
                            // 这里需要背压与等待式批处理
                            scriptScheduler.schedule(scriptId, () -> scriptExecutor.execute(scriptId, text));
                        }
                    }
                }
            } catch (Throwable e){
                // 释放资源
                for (ExcerptTailer tailer : tailers) {
                    try{
                        tailer.close();
                    }catch (Throwable t){
                        log.error("关闭 Tailer 错误！", t);
                    }
                }
                if (!(e instanceof InterruptedException)){
                    log.error("脚本执行错误！", e);
                }
            }
            future.complete(null);
        });

        scriptFilterMap.put(scriptId, new ScriptHandler(scriptId, databases, tables, thread, future));

    }

    QueueObject getQueueObject(ListenDatabaseInstanceConfig databaseInstanceConfig) {

        return queueMap.computeIfAbsent(databaseInstanceConfig, _ -> {
            SingleChronicleQueue queue = SingleChronicleQueueBuilder
                .binary(systemConfig.getQueueDirectory())
                .rollCycle(RollCycles.FAST_HOURLY)
                .build();
            return new QueueObject(queue, queue.acquireAppender());
        });
    }


    public void handle(ListenDatabaseInstanceConfig databaseInstanceConfig, ChangeEvent<String, String> record) {
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
//        JSONObject source = payloadBody.getJSONObject("source");
//        String db = source.getString("db");
        // 防止内存队列爆炸，这里快速写入本地队列
        QueueObject queueObject = getQueueObject(databaseInstanceConfig);
        queueObject.excerptAppender.writeText(value);
    }
}
