package cn.xtmax.graalcdc.config;

import org.springframework.stereotype.Component;

/**
 * 系统配置
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/29 09:26
 */
@Component
public class SystemConfig {


    public String getRuntimeDirectory() {
        return System.getProperty("user.dir") + "/data";
    }

    public String getScriptDirectory() {
        return getRuntimeDirectory() + "/scripts";
    }

    public String getLogDirectory() {
        return getRuntimeDirectory() + "/logs";
    }

    public String getOffsetDirectory() {
        return getRuntimeDirectory() + "/offset";
    }

    public String getQueueDirectory() {
        return getRuntimeDirectory() + "/queue";
    }
}

