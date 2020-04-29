package com.loeyae.tools.es_utils.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * elastic search utils.
 *
 * @date 2020-02-06
 * @version 1.0
 * @author zhangyi<loeyae@gmail.com>
 */
@Slf4j
@Component
public class ElasticSearchIndicesUtils {

    private static final String DEFAULT_ERROR_MSG = "ES Error: ";

    public static final int INDEX_DEFAULT_SHARDS = 5;
    public static final int INDEX_DEFAULT_REPLICAS = 1;

    public static final String DEFAULT_INDEX_TYPE = "_doc";

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引
     *
     * @param name index name
     * @return true | false
     */
    public boolean createIndex(String name) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, null);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        return filterCreateIndexResponse(createIndexResponse);
    }

    /**
     * 创建索引
     *
     * @param name     index name
     * @param settings Array of Integer
     * @return true | false
     */
    public boolean createIndex(String name, Integer[] settings) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, settings);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        return filterCreateIndexResponse(createIndexResponse);
    }

    /**
     * 创建索引
     *
     * @param name   index name
     * @param type   doc type
     * @param fields Map of fields
     * @return true | false
     */
    public boolean createIndex(String name, String type, Map<String, Object> fields) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, null);
        buildIndexMapping(createIndexRequest, type, fields);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        return filterCreateIndexResponse(createIndexResponse);
    }

    /**
     * 创建索引
     *
     * @param name   index name
     * @param fields Map of fields
     * @return true | false
     */
    public boolean createIndex(String name, Map<String, Object> fields) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, null);
        buildIndexMapping(createIndexRequest, DEFAULT_INDEX_TYPE, fields);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        return filterCreateIndexResponse(createIndexResponse);
    }

    /**
     * 创建索引
     *
     * @param name     index name
     * @param settings Array of Integer
     * @param type     doc type
     * @param fields   Map of fields
     * @return true | false
     */
    public boolean createIndex(String name, Integer[] settings, String type, Map<String, Object> fields) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, settings);
        buildIndexMapping(createIndexRequest, type, fields);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        return filterCreateIndexResponse(createIndexResponse);
    }

    /**
     * 创建索引
     *
     * @param name     index name
     * @param settings Array of Integer
     * @param fields   Map of fields
     * @return true | false
     */
    public boolean createIndex(String name, Integer[] settings, Map<String, Object> fields) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, settings);
        buildIndexMapping(createIndexRequest, DEFAULT_INDEX_TYPE, fields);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        return filterCreateIndexResponse(createIndexResponse);
    }

    /**
     * 索引是否存在
     *
     * @param name index name
     * @return true | false
     */
    public boolean indexExists(String name) {
        boolean restStatus = false;
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(name);
        try {
            restStatus = restHighLevelClient.indices().exists(getIndexRequest,
                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return restStatus;
    }

    /**
     * 删除索引
     *
     * @param name index name
     * @return true | false
     */
    public boolean deleteIndex(String name) {
        boolean restStatus = false;
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(name);
        try {
            AcknowledgedResponse deleteIndexResponse =
                    restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            restStatus = deleteIndexResponse.isAcknowledged();
        } catch (ElasticsearchException e) {
            if (RestStatus.NOT_FOUND == e.status()) {
                log.info("Index: {} not found", name);
            }
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return restStatus;
    }

    /**
     * 分析创建索引结果
     *
     * @param createIndexResponse instance of CreateIndexResponse
     * @return true | false
     */
    protected boolean filterCreateIndexResponse(CreateIndexResponse createIndexResponse) {
        if (null == createIndexResponse) {
            return false;
        }
        return createIndexResponse.isAcknowledged() && createIndexResponse.isShardsAcknowledged();
    }

    /**
     * 执行创建索引操作
     *
     * @param createIndexRequest instance of CreateIndexRequest
     * @return instance of CreateIndexResponse
     */
    protected CreateIndexResponse doCreateIndex(CreateIndexRequest createIndexRequest) {
        CreateIndexResponse createIndexResponse = null;
        try {
            createIndexResponse =
                    restHighLevelClient.indices().create(createIndexRequest,
                            RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return createIndexResponse;
    }

    /**
     * 构造CreateIndexRequest
     *
     * @param name index name
     * @return instance of CreateIndexRequest
     */
    protected CreateIndexRequest buildCreateIndexRequest(String name) {
        assert StringUtils.isNotEmpty(name);
        return new CreateIndexRequest(name);
    }

    /**
     * 构造index setting
     *
     * @param indexRequest instance of CreateIndexRequest
     * @param settings Array of Integer
     */
    protected void buildIndexSetting(CreateIndexRequest indexRequest, Integer[] settings) {
        Integer shards = INDEX_DEFAULT_SHARDS;
        Integer replicas = INDEX_DEFAULT_REPLICAS;
        if (null != settings) {
            if (settings.length > 1) {
                replicas = settings[1];
            }
            shards = settings[0];
        }
        indexRequest.settings(Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
        );
    }

    /**
     * 构造mapping
     *
     * @param createIndexRequest instance of CreateIndexRequest
     * @param type               doc type
     * @param fields             Map of fields
     */
    protected void buildIndexMapping(CreateIndexRequest createIndexRequest, String type, Map<String,
            Object> fields) {
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.startObject(type);
            builder.startObject("properties");
            buildIndexFields(builder, fields);
            builder.endObject();
            builder.endObject();
            builder.endObject();
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        assert builder != null;
        createIndexRequest.mapping(type, builder);
    }

    /**
     * 构造字段集
     *
     * @param builder instance of XContentBuilder
     * @param fields  Map of fields
     */
    @SuppressWarnings("unchecked")
    protected void buildIndexFields(XContentBuilder builder, Map<String, Object> fields) {
        fields.forEach((key, value) -> {
            try {
                builder.startObject(key);
                if (value instanceof Map) {
                    ((Map<Object, Object>) value).forEach((k, v)->{
                        if (v instanceof Map) {
                            Map<String, Object> map = new HashMap<>();
                            map.put(String.valueOf(k), v);
                            buildIndexFields(builder, map);
                        } else {
                            try {
                                builder.field(String.valueOf(k), v);
                            } catch (Exception e) {
                                log.error(DEFAULT_ERROR_MSG, e);
                            }
                        }
                    });
                } else {
                    builder.field("type", value);
                }
                builder.endObject();
            } catch (IOException e) {
                log.error(DEFAULT_ERROR_MSG, e);
            }
        });
    }
}