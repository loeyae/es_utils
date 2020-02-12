package com.loeyae.tools.es_utils.component;

import com.loeyae.tools.es_utils.common.ElasticSearchQueryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Level;

/**
 * Elastic Search Document Utils.
 *
 * @date: 2020-02-06
 * @version: 1.0
 * @author: zhangyi07@beyondsoft.com
 */
@Slf4j
@Component
public class ElasticSearchDocumentUtils {

    private static final String DEFAULT_ERROR_MSG = "ES Error: ";

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * 新增
     *
     * @param index
     * @param type
     * @param source
     * @return
     */
    public boolean insert(String index, String type, Map<String, Object> source) {
        System.out.print(restHighLevelClient);
        IndexRequest indexRequest = new IndexRequest(index, type);
        indexRequest.source(source);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest,
                    RequestOptions.DEFAULT);
            if (RestStatus.CREATED == indexResponse.status()) {
                return true;
            }
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return false;
    }


    /**
     * 新增
     *
     * @param index
     * @param type
     * @param source
     * @return
     * @para id
     */
    public boolean insert(String index, String type, String id, Map<String, Object> source) {
        IndexRequest indexRequest = new IndexRequest(index, type, id);
        indexRequest.source(source);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest,
                    RequestOptions.DEFAULT);
            if (RestStatus.CREATED == indexResponse.status()) {
                return true;
            }
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return false;
    }

    /**
     * 获取
     *
     * @param index
     * @param type
     * @param id
     * @return
     */
    public Map<String, Object> get(String index, String type, String id) {
        GetRequest getRequest = new GetRequest(index, type, id);
        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
            return getResponse.getSourceAsMap();
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return null;
    }

    /**
     * 更新
     *
     * @param index
     * @param type
     * @param id
     * @param source
     * @return
     */
    public boolean update(String index, String type, String id, Map<String, Object> source) {
        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.doc(source);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest,
                    RequestOptions.DEFAULT);
            if (RestStatus.OK == updateResponse.status() && DocWriteResponse.Result.UPDATED == updateResponse.getResult()) {
                return true;
            }
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return false;
    }

    /**
     * 更新
     *
     * @param index
     * @param type
     * @param id
     * @param script
     * @return
     */
    public boolean update(String index, String type, String id, Script script) {
        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.script(script);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest,
                    RequestOptions.DEFAULT);
            if (RestStatus.OK == updateResponse.status() && DocWriteResponse.Result.UPDATED == updateResponse.getResult()) {
                return true;
            }
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return false;
    }

    /**
     * 删除
     *
     * @param index
     * @param type
     * @param id
     * @return
     */
    public boolean delete(String index, String type, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest,
                    RequestOptions.DEFAULT);
            if (RestStatus.OK == deleteResponse.status() && DocWriteResponse.Result.DELETED == deleteResponse.getResult()) {
                return true;
            }
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return false;
    }

    /**
     * 批量添加
     *
     * @param index
     * @param type
     * @param sources
     * @return
     */
    public boolean[] bulkInsert(String index, String type, List<Map<String, Object>> sources) {
        BulkRequest bulkRequest = new BulkRequest();
        sources.forEach(item -> {
            bulkRequest.add(new IndexRequest(index, type).source(item));
        });
        try {
            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            boolean[] restStatus = new boolean[sources.size()];
            int i = 0;
            for (BulkItemResponse item : bulkResponse) {
                restStatus[i] = (RestStatus.CREATED == item.status() ? true : false);
                i++;
            }
            return restStatus;
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return null;
    }

    /**
     * 根据search条件更新数据
     *
     * @param index
     * @param type
     * @param script
     * @param search
     * @return
     */
    public long updateByQuery(String index, String type, Script script, Map<String,
            Object> search) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(index);
        updateByQueryRequest.setDocTypes(type);
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(search);
        updateByQueryRequest.setQuery(queryBuilder);
        updateByQueryRequest.setScript(script);
        try {
            BulkByScrollResponse bulkResponse =
                    restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
            return bulkResponse.getUpdated();
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return 0L;
    }

    /**
     * 根据search条件更新数据
     *
     * @param index
     * @param type
     * @param doc
     * @param search
     * @return
     */
    public long updateByQuery(String index, String type, Map<String, Object> doc, Map<String,
            Object> search) {
        Settings.Builder settingsBuilder = Settings.builder();
        doc.entrySet().forEach(item -> {
            buildSettingsElement(settingsBuilder, item.getKey());
        });
        Settings settings = settingsBuilder.build();
        String source = settings.toDelimitedString(';');;
        System.out.println(source);
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, source
                , doc);
        return updateByQuery(index, type, script, search);
    }

    /**
     * 构建search参数
     *
     * @param builder
     * @param key
     */
    static public void buildSettingsElement(Settings.Builder builder, String key) {
        builder.put("ctx._source."+ key, "params."+key);
    }

    /**
     * 更具search条件删除数据
     *
     * @param index
     * @param type
     * @param search
     * @return
     */
    public long deleteByQuery(String index, String type, Map<String, Object> search) {
        assert null != search;
        assert search.size() > 0;
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
        deleteByQueryRequest.setDocTypes(type);
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(search);
        deleteByQueryRequest.setQuery(queryBuilder);
        try {
            BulkByScrollResponse bulkResponse =
                    restHighLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
            return bulkResponse.getDeleted();
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return 0L;
    }

}