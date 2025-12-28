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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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

    private final List<Context> contexts = new CopyOnWriteArrayList<>();

    // 全局共享：编译后的源代码（线程安全）
    private final Map<String, Source> sourceCache = new ConcurrentHashMap<>();

    // 全局共享：代码引擎（提升跨 Context 的 JIT 性能）
    private final Engine sharedEngine = Engine.newBuilder().build();

    // 线程局部：脚本上下文
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


    // 线程局部：缓存特定线程 Context 下的 Value
    private final ThreadLocal<Map<String, Value>> functionCache = ThreadLocal.withInitial(HashMap::new);


    /**
     * 执行脚本
     *
     * @param filepath 脚本文件路径
     * @param args     脚本参数
     */
    public void execute(String filepath, Object... args) {
        Context ctx = threadContext.get();
        Map<String, Value> cache = functionCache.get();

        // 获取或初始化该线程专属的 Value
        Value asyncFunc = cache.computeIfAbsent(filepath, path -> {
            Source source = sourceCache.computeIfAbsent(path, p -> {
                try {
                    return Source.newBuilder("js", new File(p))
                        .mimeType("application/javascript+module")
                        .build();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            // eval 后获取该线程 Context 下的 export default
            return ctx.eval(source).getMember("default");
        });
        Value promise = asyncFunc.execute(args);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        if (promise.hasMember("then")) {
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
                throw new IllegalStateException( "JS 异步执行出错: " + errorRef.get().getMessage(), errorRef.get());
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
}
