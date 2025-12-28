package cn.xtmax.graalcdc.config;

import cn.xtmax.graalcdc.DbType;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

/**
 * 监听数据库实例的配置
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/27 20:06
 */
@Getter
@Setter
public class ListenDatabaseInstanceConfig {

    // 实例名称
    private String name;

    // 数据库类型
    private DbType dbType;

    // 监听的数据库
    private Set<String> databases;

    // 监听的表
    private Set<String> tables;

    // 连接主机
    private String host;

    // 端口
    private int port;

    // 用户信息
    private String username;

    // 密码
    private String password;

    public String uniqueKey(){
        return dbType.getValue() + ":" + host + ":" + port + "@" + username + ":" + password;
    }
}
