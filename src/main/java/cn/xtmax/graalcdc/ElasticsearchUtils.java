package cn.xtmax.graalcdc;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.HostAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Elasticsearch 工具类
 *
 * @author 起风了
 * @email m.zxt@foxmail.com
 * @date 2025/12/28 12:28
 */
@Slf4j
@Component
public class ElasticsearchUtils {

    @Autowired
    private WebClient webClient;

    @Autowired
    ConversionService conversionService;

    <T> T requireParamValue(Map<String, ?> params, String key, Class<T> clazz) {
        T value = getParamValue(params, key, clazz);
        if (value == null) {
            throw new IllegalArgumentException("参数 " + key + " 的值不能为空");
        }
        return value;
    }

    <T> T getParamValue(Map<String, ?> params, String key, Class<T> clazz) {
        if (!params.containsKey(key)) {
            throw new IllegalArgumentException("参数缺少 " + key + " 字段");
        }
        return conversionService.convert(params.get(key), clazz);
    }

    String urlEncode(String text){
        return URLEncoder.encode(text, StandardCharsets.UTF_8);
    }

    enum Action{
        PUT,
        DELETE
    }


    @SuppressWarnings("unchecked")
    public List<? extends Map<String, ?>> asMapList(Object value){
        if (value == null){
            return Collections.emptyList();
        }
        if (value.getClass().isArray()){
            return Stream.of((Object[]) value)
                .map(v -> {
                    if (!(v instanceof Map)){
                        throw new IllegalArgumentException("参数 records 的元素必须是Map<String, Object>");
                    }
                    return (Map<String, ?>) v;
                })
                .toList();
        }
        if (value instanceof List<?>){
            return ((List<?>) value).stream()
                .map(v -> {
                    if (!(v instanceof Map)){
                        throw new IllegalArgumentException("参数 records 的元素必须是Map<String, Object>");
                    }
                    return (Map<String, ?>) v;
                })
                .toList();
        }
        if (!(value instanceof Map)){
            throw new IllegalArgumentException("参数 records 的元素必须是Map<String, Object>");
        }
        return List.of((Map<String, ?>) value);
    }


    /**
     * 调用 Elasticsearch
     *
     * @param action 请求动作
     * @param params 参数
     * @return 响应结果
     */
    @SuppressWarnings("unchecked")
    CompletableFuture<String> invoke(Action action, Map<String, Object> params) {
        if (params == null) {
            throw new IllegalArgumentException("参数不能为空");
        }
        String url = requireParamValue(params, "url", String.class);
        String username = requireParamValue(params, "username", String.class);
        String password = requireParamValue(params, "password", String.class);
        if (url.isBlank()){
            throw new IllegalArgumentException("参数 url 不能为空");
        }
        if (username.isBlank()){
            throw new IllegalArgumentException("参数 username 不能为空");
        }
        if (password.isBlank()){
            throw new IllegalArgumentException("参数 password 不能为空");
        }
        String idKey = requireParamValue(params, "idKey", String.class);
        if (idKey.isBlank()){
            throw new IllegalArgumentException("参数 idKey 不能为空");
        }
        String basicValue = Base64.getEncoder()
            .encodeToString((urlEncode(username) + ":" + urlEncode(password)).getBytes());
        List<? extends Map<String, ?>> records = asMapList(params.get("records"));
        if (records.isEmpty()){
            if (log.isDebugEnabled()){
                log.debug("参数 records 为空，返回空future");
            }
            return CompletableFuture.completedFuture("");
        }
        return switch (action) {
            case PUT -> {
                WebClient.RequestBodySpec spec;
                if (records.size() > 1){
                    StringBuilder sb = new StringBuilder();
                    for (Map<String, ?> record : records) {
                        Object id = getParamValue(record, idKey, Object.class);
                        sb.append("{\"index\":{\"_id\":").append(JSONObject.toJSONString(id)).append("}}\n");
                        sb.append(JSONObject.toJSONString(record)).append("\n");
                    }
                    spec = webClient.post()
                        .uri(url + "/_bluk")
                        .header("Content-Type", "application/x-ndjson");
                    spec.bodyValue(sb.toString());
                }else{
                    Map<String, ?> first = records.getFirst();
                    Object id = getParamValue(first, idKey, Object.class);
                    spec = webClient.put()
                        .uri(url + "/_doc/" + id)
                        .header("Content-Type", "application/json");
                    spec.bodyValue(JSONObject.toJSONString(first));
                }
                spec.header("Authorization", "Basic " + basicValue);
                yield spec.retrieve().bodyToMono(String.class).toFuture();
            }
            case DELETE -> {
                if (records.size() > 1){
                    StringBuilder sb = new StringBuilder();
                    for (Map<String, ?> record : records) {
                        Object id = getParamValue(record, idKey, Object.class);
                        sb.append("{\"delete\":{\"_id\":").append(JSONObject.toJSONString(id)).append("}}\n");
                    }
                    yield webClient.post()
                        .uri(url + "/_bluk")
                        .header("Content-Type", "application/x-ndjson")
                        .header("Authorization", "Basic " + basicValue)
                        .bodyValue(sb.toString()).retrieve().bodyToMono(String.class).toFuture();
                }else{
                    Map<String, Object> first = (Map<String, Object>) records.getFirst();
                    Object id = getParamValue(first, idKey, Object.class);
                    yield webClient.delete()
                        .uri(url + "/_doc/" + id)
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Basic " + basicValue)
                        .retrieve().bodyToMono(String.class).toFuture();
                }
            }
        };
    }

    @HostAccess.Export
    public CompletableFuture<String> put(Map<String, Object> params) {
        return invoke(Action.PUT, params);
    }

    @HostAccess.Export
    public CompletableFuture<String> delete(Map<String, Object> params) {
        return invoke(Action.DELETE, params);
    }
}
