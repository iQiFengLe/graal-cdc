package cn.xtmax.graalcdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 文件监听
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/29 09:30
 */
public class WatchFile implements AutoCloseable {

    final WatchService watchService;

    final Executor listenExecutor;

    final Map<Path, List<Listener>> listenerMap = new ConcurrentHashMap<>();

    final Map<WatchKey, Path> watchKeyPathMap = new ConcurrentHashMap<>();

    // 用于防抖处理的调度线程池
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // 追踪每个文件的延迟任务，Key 是文件绝对路径
    final Map<Path, ScheduledFuture<?>> debounceMap = new ConcurrentHashMap<>();

    private volatile boolean closed;

    @FunctionalInterface
    public interface ListenerHandler {

        void handle(WatchEvent.Kind<?> kind, Path listenerPath, Path absolutePath);

    }


    record Listener(WatchKey watchKey, ListenerHandler handler, Set<WatchEvent.Kind<?>> kinds) {
        Listener(WatchKey watchKey, ListenerHandler handler, WatchEvent.Kind<?>... kinds) {
            this(watchKey, handler, Set.of(kinds));
        }
    }

    /**
     * 创建监听服务
     *
     * @param listenExecutor 监听执行器
     */
    public WatchFile(Executor listenExecutor) {
        try {
            this.watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            throw new RuntimeException("创建监听服务失败！", e);
        }
        this.listenExecutor = listenExecutor == null ? Executors.newSingleThreadExecutor() : listenExecutor;
        listener();
    }
    public WatchFile() {
        this(Executors.newSingleThreadExecutor());
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        try {
            watchService.close();
        } finally {
            if (listenExecutor instanceof ExecutorService e) {
                e.shutdownNow();
            } else if (listenExecutor instanceof AutoCloseable a) {
                a.close();
            }
            scheduler.shutdown();
        }
    }

    private void submitTask(Path path, WatchEvent.Kind<?> kind, Path dirPath, List<Listener> listeners) {
        // 如果该文件已有待执行任务，取消它
        ScheduledFuture<?> scheduledTask = debounceMap.get(path);
        if (scheduledTask != null && !scheduledTask.isDone()) {
            scheduledTask.cancel(false);
        }
        // 提交一个新的延迟任务
        ScheduledFuture<?> newTask = scheduler.schedule(() -> {
            try {
                // 真正执行监听回调逻辑
                for (Listener listener : listeners) {
                    if (listener.kinds.isEmpty() || listener.kinds.contains(kind)) {
                        listener.handler.handle(kind, dirPath, path);
                    }
                }
            } finally {
                debounceMap.remove(path);
            }
        }, 500, TimeUnit.MILLISECONDS);
        debounceMap.put(path, newTask);
    }

    private void listener() {

        listenExecutor.execute(() -> {
            while (!closed) {
                WatchKey watchKey;
                try {
                    watchKey = watchService.take();
                } catch (InterruptedException | ClosedWatchServiceException e) {
                    // 服务中断
                    break;
                }
                if (watchKey == null) {
                    continue;
                }
                List<WatchEvent<?>> watchEvents = watchKey.pollEvents();
                for (WatchEvent<?> watchEvent : watchEvents) {

                    WatchEvent.Kind<?> kind = watchEvent.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        // 文件监听时发生了事件溢出，跳过
                        continue;
                    }
                    // 获取受影响的文件名
                    @SuppressWarnings("unchecked")
                    Path relativePath = ((WatchEvent<Path>) watchEvent).context();

                    Path dirPath = watchKeyPathMap.get(watchKey);

                    Path absolutePath = dirPath.resolve(relativePath);
                    // 防止误触
                    if (!Files.exists(absolutePath)) {
                        continue;
                    }

                    List<Path> list = Arrays.asList(dirPath, absolutePath);
                    for (Path path : list) {
                        if (listenerMap.containsKey(path)) {
                            submitTask(absolutePath, kind, dirPath, new ArrayList<>(listenerMap.get(path)));
                        }
                    }
                }
                if (!watchKey.reset()) {
                    watchKey.cancel();
                    Set<Map.Entry<Path, List<Listener>>> entries = listenerMap.entrySet();
                    for (Map.Entry<Path, List<Listener>> entry : entries) {
                        List<Listener> listeners = entry.getValue();
                        listeners.removeIf(listener -> listener.watchKey == watchKey);
                        if (listeners.isEmpty()) {
                            listenerMap.remove(entry.getKey());
                        }
                    }
                }
            }
        });

    }


    public WatchKey watch(String path, ListenerHandler handler, WatchEvent.Kind<?>... eventKinds) throws IOException {
        File dir = new File(path);
        while (dir != null && !dir.exists()) {
            dir = dir.getParentFile();
        }
        if (dir == null) {
            throw new IOException("任意一级路径都不存在！" + path);
        }
        Path filePath = Paths.get(path);

        if (dir.isFile()) {
            dir = dir.getParentFile();
        }
        Path dirPath = dir.toPath();
        WatchKey watchKey = dirPath.register(watchService, eventKinds);
        listenerMap.computeIfAbsent(filePath, _ -> new CopyOnWriteArrayList<>()).add(new Listener(watchKey, handler, eventKinds));
        watchKeyPathMap.put(watchKey, dirPath);
        return watchKey;
    }

    public WatchKey watch(Path path, ListenerHandler handler, WatchEvent.Kind<?>... eventKinds) throws IOException {
        return watch(path.toString(), handler, eventKinds);
    }

}
