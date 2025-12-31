package cn.xtmax.graalcdc;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.*;

/**
 * 脚本调度器
 *
 * @author 起风了
 * @mail m.zxt@foxmail.com
 * @date 2025/12/29 19:00
 */
@Component
public class ScriptScheduler implements AutoCloseable{


    /**
     * 线程池
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * 串行控制
     */
    final Map<String, Semaphore> semaphoreMap = new ConcurrentHashMap<>();

    /**
     * 运行脚本
     *
     * @param scriptId      脚本ID
     * @param runnable      脚本
     * @param scriptSerial  脚本是否串行执行
     * @return 脚本执行结果
     */
    public CompletableFuture<?> schedule(String scriptId, Runnable runnable, boolean scriptSerial) throws InterruptedException {
        if (scriptSerial) {
            Semaphore semaphore = semaphoreMap.computeIfAbsent(scriptId, _ -> new Semaphore(1));
            semaphore.acquire();
            return CompletableFuture.runAsync(() -> {
                try {
                    runnable.run();
                } finally {
                    semaphore.release();
                }
            }, executor);
        } else {
            return CompletableFuture.runAsync(runnable, executor);
        }
    }

    /**
     * 串行运行脚本
     *
     * @param scriptId  脚本ID
     * @param runnable  脚本
     * @return 脚本执行结果
     */
    public CompletableFuture<?> schedule(String scriptId, Runnable runnable) throws InterruptedException {
        return schedule(scriptId, runnable, true);
    }

    /**
     * 并行运行脚本
     *
     * @param scriptId  脚本ID
     * @param runnable  脚本
     * @return 脚本执行结果
     */
    public CompletableFuture<?> parallelSchedule(String scriptId, Runnable runnable) throws InterruptedException {
        return schedule(scriptId, runnable, false);
    }


    @Override
    public void close() throws Exception {
        executor.shutdown();
    }
}
