package cn.xtmax.graalcdc;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 脚本调度器
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/29 19:00
 */
@Component
public class ScriptScheduler implements AutoCloseable{


    /**
     * 线程池
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * 运行脚本
     *
     * @param task      任务
     * @return 脚本执行结果
     */
    public CompletableFuture<?> schedule(Runnable task) {
        return CompletableFuture.runAsync(task, executor);
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }
}
