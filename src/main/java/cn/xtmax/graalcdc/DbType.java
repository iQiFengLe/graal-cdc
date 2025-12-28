package cn.xtmax.graalcdc;

/**
 * 数据库类型枚举
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/27 20:00
 */
public enum DbType {

    MYSQL("mysql"),
    POSTGRESQL("postgresql"),
    ;
    private final String value;
    DbType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
