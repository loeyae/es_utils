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
 * @date: 2020-02-06
 * @version: 1.0
 * @author: zhangyi07@beyondsoft.com
 */
@Slf4j
@Component
public class ElasticSearchIndicesUtils {

    private static final String DEFAULT_ERROR_MSG = "ES Error: ";

    private static final int INDEX_DEFAULT_SHARDS = 5;
    private static final int INDEX_DEFAULT_REPLICAS = 1;

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引
     *
     * @param name
     * @return
     */
    public boolean createIndex(String name) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, null);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        boolean restStatus = filterCreateIndexResponse(createIndexResponse);
        return restStatus;
    }

    /**
     * 创建索引
     *
     * @param name
     * @param settings
     * @return
     */
    public boolean createIndex(String name, Integer[] settings) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, settings);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        boolean restStatus = filterCreateIndexResponse(createIndexResponse);
        return restStatus;
    }

    /**
     * 创建索引
     *
     * @param name
     * @param type
     * @param fields
     * @return
     */
    public boolean createIndex(String name, String type, Map<String, Object> fields) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, null);
        buildIndexMapping(createIndexRequest, type, fields);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        boolean restStatus = filterCreateIndexResponse(createIndexResponse);
        return restStatus;
    }

    /**
     * 创建索引
     *
     * @param name
     * @param settings
     * @param type
     * @param fields
     * @return
     */
    public boolean createIndex(String name, Integer[] settings, String type, Map<String, Object> fields) {
        CreateIndexRequest createIndexRequest = buildCreateIndexRequest(name);
        buildIndexSetting(createIndexRequest, settings);
        buildIndexMapping(createIndexRequest, type, fields);
        CreateIndexResponse createIndexResponse = doCreateIndex(createIndexRequest);
        boolean restStatus = filterCreateIndexResponse(createIndexResponse);
        return restStatus;
    }

    /**
     * 索引是否存在
     *
     * @param name
     * @return
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
     * @param name
     * @return
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
     * @param createIndexResponse
     * @return
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
     * @param createIndexRequest
     * @return
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
     * @param name 索引名称
     * @return
     */
    protected CreateIndexRequest buildCreateIndexRequest(String name) {
        assert StringUtils.isNotEmpty(name);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(name);
        return createIndexRequest;
    }

    /**
     * 构造index setting
     *
     * @param indexRequest
     * @param settings
     */
    protected void buildIndexSetting(CreateIndexRequest indexRequest,
                                     Integer[] settings) {
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
     * @param createIndexRequest
     * @param type
     * @param fields
     */
    protected void buildIndexMapping(CreateIndexRequest createIndexRequest, String type, Map<String,
            Object> fields) {
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject(type);
                {
                    builder.startObject("properties");
                    {
                        buildIndexFields(builder, fields);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        createIndexRequest.mapping(type, builder);
    }

    /**
     * 构造字段集
     *
     * @param builder
     * @param fields
     */
    protected void buildIndexFields(XContentBuilder builder,
                                    Map<String, Object> fields) {
        fields.forEach((key, value) -> {
            try {
                builder.startObject(key);
                {
                    if (value instanceof Map) {
                        ((Map) value).forEach((k, v)->{
                            if (v instanceof Map) {
                                buildIndexFields(builder, new HashMap<String, Object>(){{
                                    put(String.valueOf(k), v);
                                }});
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
                }
                builder.endObject();
            } catch (IOException e) {
                log.error(DEFAULT_ERROR_MSG, e);
            }
        });
    }
}