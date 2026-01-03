package cn.xtmax.graalcdc;

import cn.xtmax.graalcdc.config.SystemConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.List;

/**
 * 脚本容器
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/29 09:20
 */
@Component
public class ScriptContainer implements InitializingBean {

    @Autowired
    SystemConfig systemConfig;

    @Autowired
    ScriptExecutor scriptExecutor;

    @Autowired
    WatchFile watchFile;


    public static List<File> getScriptFiles(String directory){
        File[] files = new File(directory).listFiles();
        List<File> scripts = new ArrayList<>();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".js") || file.getName().endsWith(".mjs")) {
                    scripts.add(file);
                } else if (file.isDirectory()) {
                    File[] subFiles = file.listFiles();
                    // 子目录要求为：
                    // xxx/index.js
                    // xxx/index.mjs
                    if (subFiles != null) {
                        for (File subFile : subFiles) {
                            if (subFile.isFile() && subFile.getName().equals("index.js") || subFile.getName().equals("index.mjs")) {
                                scripts.add(subFile);
                            }
                        }
                    }
                }
            }
        }
        return scripts;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        String directory = systemConfig.getScriptDirectory();
        // 加载所有脚本
        List<File> scriptFiles = getScriptFiles(directory);
        for (File file : scriptFiles) {
            // 预热脚本
            scriptExecutor.register(file.getAbsolutePath());
        }
        // 监听脚本目录
        watchFile.watch(directory, (WatchEvent.Kind<?> kind, Path _, Path path) -> {
                if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    scriptExecutor.unregister(path.toString());
                } else {
                    scriptExecutor.reload(path.toString());
                }
            },
            StandardWatchEventKinds.ENTRY_CREATE,
            StandardWatchEventKinds.ENTRY_MODIFY,
            StandardWatchEventKinds.ENTRY_DELETE,
            StandardWatchEventKinds.OVERFLOW
        );
    }
}
