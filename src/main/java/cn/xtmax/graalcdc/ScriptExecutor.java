package cn.xtmax.graalcdc;

import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 脚本执行器
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/27 20:46
 */
@Slf4j
@Component
public class ScriptExecutor implements DisposableBean {


    @Autowired
    ElasticsearchUtils elasticsearchUtils;

    // 全局共享：编译后的源代码（线程安全）
    private final Map<String, SourceObject> sourceCache = new ConcurrentHashMap<>();

    // 全局共享：代码引擎（提升跨 Context 的 JIT 性能）
    private final Engine sharedEngine = Engine.newBuilder().build();

    // 初始化脚本中的cdc配置
    private final Context initCDCContext = Context.newBuilder("js")
        .engine(sharedEngine)
        .allowAllAccess(true)
        .option("js.esm-eval-returns-exports", "true")
        .option("js.foreign-object-prototype", "true")
        .build();
    // 独立的单线程池，负责执行脚本initCDC方法
    private final ExecutorService initCDCExecutor = Executors.newSingleThreadExecutor();

    // 上下文列表，统一销毁 Context
    private final List<Context> contexts = new CopyOnWriteArrayList<>();


    record SourceObject(long version, Source source) {
    }

    record SourceFunctionObject(long version, Value function) {
    }

    // 每个线程的脚本上下文
    private final ThreadLocal<Context> threadContext = ThreadLocal.withInitial(
        () -> {
            Context context = Context.newBuilder("js")
                .engine(sharedEngine) // 绑定共享引擎
                .allowAllAccess(true)
                .option("js.esm-eval-returns-exports", "true")
                .option("js.foreign-object-prototype", "true")
                .build();
            contexts.add(context);
            context.getBindings("js").putMember("es", elasticsearchUtils);
            return context;
        }
    );

    // 当前线程正在执行的文件路径
    private final ThreadLocal<String> threadFilepath = new ThreadLocal<>();


    // 线程局部：缓存特定线程 Context 下的 Value
    private final ThreadLocal<Map<String, SourceFunctionObject>> functionCache = ThreadLocal.withInitial(HashMap::new);

    static boolean isPromise(Value value) {
        return value != null && (
            value.getMetaObject().getMetaSimpleName().equals("Promise") || value.hasMember("then")
        );
    }

    /**
     * 初始化脚本中的cdc配置，例如订阅数据库，期望监听的表、库等
     *
     * @param jsSource 脚本源
     */
    private void initScriptCDC(Context ctx, Source jsSource) {
        Value esmNamespaces = ctx.eval(jsSource);
        Value defaultFunc = esmNamespaces.getMember("default");
        if (defaultFunc == null || !defaultFunc.hasMember("then")) {
            throw new IllegalStateException("JS 异步执行出错: 缺少export default async 方法");
        }
        Value initFunc = esmNamespaces.getMember("initCDC");
        Value value = initFunc.execute();
        if (isPromise(value)) {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<IllegalStateException> errorRef = new AtomicReference<>();
            value.invokeMember("then", (ProxyExecutable) _ -> {
                latch.countDown();
                return null;
            }).invokeMember("catch", (ProxyExecutable) ar -> {
                // 捕获 JS 中的异步错误，方便 Java 侧抛出
                errorRef.set(new IllegalStateException("JS 异步执行出错: " + ar[0].toString()));
                return null;
            });
            try {
                latch.await();
                if (errorRef.get() != null) {
                    throw errorRef.get();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private SourceObject getSourceObject(String filepath) {
        return sourceCache.computeIfAbsent(filepath, p -> {
            try {
                Source jsSource = Source.newBuilder("js", new File(p))
                    .mimeType("application/javascript+module")
                    .build();
                // 初始化脚本中的cdc配置
                // 阻塞到执行完毕
                CompletableFuture.runAsync(() -> initScriptCDC(initCDCContext, jsSource), initCDCExecutor).join();
                return new SourceObject(System.currentTimeMillis(), jsSource);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

    private Value getDefaultAsyncFunc(String filepath) {
        Context ctx = threadContext.get();
        Map<String, SourceFunctionObject> cache = functionCache.get();
        SourceObject sourceObject = getSourceObject(filepath);
        // 获取或初始化该线程专属的 Value
        SourceFunctionObject asyncFuncObject = cache.computeIfAbsent(filepath, _ -> {
            // eval 后获取该线程 Context 下的 export default
            return new SourceFunctionObject(sourceObject.version, ctx.eval(sourceObject.source).getMember("default"));
        });
        if (asyncFuncObject.version != sourceObject.version) {
            cache.remove(filepath);
            return cache.computeIfAbsent(filepath, _ -> {
                // eval 后获取该线程 Context 下的 export default
                return new SourceFunctionObject(sourceObject.version, ctx.eval(sourceObject.source).getMember("default"));
            }).function;
        }
        return asyncFuncObject.function;
    }

    /**
     * 执行脚本
     *
     * @param filepath 脚本文件路径
     * @param args     脚本参数
     */
    public void execute(String filepath, Object... args) {
        Value defaultAsyncFunc = getDefaultAsyncFunc(filepath);
        threadFilepath.set(filepath);
        Value promise;
        try {
            promise = defaultAsyncFunc.execute(args);
        } finally {
            threadFilepath.remove();
        }
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        if (isPromise(promise)) {
            promise.invokeMember("then", (ProxyExecutable) _ -> {
                latch.countDown();
                return null;
            }).invokeMember("catch", (ProxyExecutable) ar -> {
                // 捕获 JS 中的异步错误，方便 Java 侧抛出
                errorRef.set(new IllegalStateException("JS 异步执行出错: " + ar[0].toString()));
                latch.countDown();
                return null;
            });
        } else {
            throw new IllegalStateException("JS 异步执行出错: 缺少 then 方法");
        }
        try {
            latch.await();
            if (errorRef.get() != null) {
                throw new IllegalStateException("JS 异步执行出错: " + errorRef.get().getMessage(), errorRef.get());
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("线程被中断", e);
        }
    }

    @Override
    public void destroy() throws Exception {
        for (Context context : contexts) {
            try {
                context.close(true);
            } catch (Exception e) {
                log.error("关闭 Context 失败！", e);
            }
        }
        try {
            sharedEngine.close();
        } catch (Exception e) {
            log.error("关闭 Engine 失败！", e);
        }
        sourceCache.clear();
    }

    /**
     * 重新加载脚本
     *
     * @param filepath 脚本文件路径
     */
    public void reload(String filepath) {
        sourceCache.remove(filepath);
    }

    /**
     * 注册脚本
     *
     * @param filepath 脚本文件路径
     */
    public void register(String filepath) {
        sourceCache.remove(filepath);
        getSourceObject(filepath);
    }

    /**
     * 注销脚本
     *
     * @param filepath 脚本文件路径
     */
    public void unregister(String filepath) {
        sourceCache.remove(filepath);
    }
}
