package com.loeyae.tools.es_utils.component;

import com.loeyae.tools.es_utils.common.ElasticSearchQueryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Elastic Search Document Utils.
 *
 * @date 2020-02-06
 * @version 1.0
 * @author zhangyi<loeyae@gmail.com>
 */
@Slf4j
@Component
public class ElasticSearchDocumentUtils {

    private static final String DEFAULT_ERROR_MSG = "ES Error: ";

    private static final int MAX_RETRY_TIMES = 5;

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * 新增
     *
     * @param index  index name
     * @param type   doc type
     * @param source source data
     * @return doc id | null
     */
    public String insert(String index, String type, Map<String, Object> source) {
        IndexRequest indexRequest = new IndexRequest(index, type);
        indexRequest.source(source);
        return insert(indexRequest);
    }

    /**
     * 新增
     *
     * @param index  index name
     * @param source source data
     * @return doc id | null
     */
    public String insert(String index, Map<String, Object> source) {
        IndexRequest indexRequest = new IndexRequest(index, ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE);
        indexRequest.source(source);
        return insert(indexRequest);
    }

    /**
     * 新增
     *
     * @param index  index name
     * @param type   doc type
     * @param id     doc id
     * @param source source data
     * @return doc id | null
     */
    public String insert(String index, String type, String id, Map<String, Object> source) {
        IndexRequest indexRequest = new IndexRequest(index, type, id);
        indexRequest.source(source);
        return insert(indexRequest);
    }

    /**
     * insert
     *
     * @param indexRequest instance of IndexRequest
     * @return doc id | null
     */
    public String insert(IndexRequest indexRequest) {
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest,
                    RequestOptions.DEFAULT);
            if (RestStatus.CREATED == indexResponse.status()) {
                return indexResponse.getId();
            }
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return null;
    }

    /**
     * 获取
     *
     * @param index index name
     * @param id    doc id
     * @return Map of doc data
     */
    public Map<String, Object> get(String index, String id) {
        GetRequest getRequest = new GetRequest(index, ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE, id);
        return get(getRequest);
    }

    /**
     * 获取
     *
     * @param index index name
     * @param type  doc type
     * @param id    doc id
     * @return Map of doc data
     */
    public Map<String, Object> get(String index, String type, String id) {
        GetRequest getRequest = new GetRequest(index, type, id);
        return get(getRequest);
    }

    /**
     * get
     *
     * @param getRequest instance of GetRequest
     * @return Map of doc data | null
     */
    public Map<String, Object> get(GetRequest getRequest) {
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
     * @param index  index name
     * @param id     doc id
     * @param source source data
     * @return true | false
     */
    public boolean update(String index, String id, Map<String, Object> source) {
        UpdateRequest updateRequest = new UpdateRequest(index, ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE, id);
        updateRequest.doc(source);
        return update(updateRequest);
    }

    /**
     * 更新
     *
     * @param index  index name
     * @param type   doc type
     * @param id     doc id
     * @param source source data
     * @return true | false
     */
    public boolean update(String index, String type, String id, Map<String, Object> source) {
        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.doc(source);
        return update(updateRequest);
    }

    /**
     * 更新
     *
     * @param index  index name
     * @param id     doc id
     * @param script instance of Script
     * @return true | false
     */
    public boolean update(String index, String id, Script script) {
        UpdateRequest updateRequest = new UpdateRequest(index, ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE, id);
        updateRequest.script(script);
        return update(updateRequest);
    }

    /**
     * 更新
     *
     * @param index  index name
     * @param type   doc type
     * @param id     doc id
     * @param script isntance of Script
     * @return true | false
     */
    public boolean update(String index, String type, String id, Script script) {
        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.script(script);
        return update(updateRequest);
    }

    /**
     * update
     *
     * @param updateRequest instance of UpdateRequest
     * @return true | false
     */
    public boolean update(UpdateRequest updateRequest) {

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
     * @param index index name
     * @param id    doc id
     * @return true | false
     */
    public boolean delete(String index, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(index, ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE, id);
        return delete(deleteRequest);
    }

    /**
     * 删除
     *
     * @param index index name
     * @param type  doc type
     * @param id    doc id
     * @return true | false
     */
    public boolean delete(String index, String type, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
        return delete(deleteRequest);
    }

    /**
     * delete
     *
     * @param deleteRequest instance of DeleteRequest
     * @return true | false
     */
    public boolean delete(DeleteRequest deleteRequest) {
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
     * @param index   index name
     * @param sources List of source data
     * @return Array of doc id
     */
    public String[] bulkInsert(String index, List<Map<String, Object>> sources) {
        BulkRequest bulkRequest = new BulkRequest();
        sources.forEach(item -> bulkRequest.add(new IndexRequest(index, ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE).source(item)));
        return bulkInsert(bulkRequest);
    }

    /**
     * 批量添加
     *
     * @param index   index name
     * @param type    doc type
     * @param sources List of source data
     * @return Array of doc id
     */
    public String[] bulkInsert(String index, String type, List<Map<String, Object>> sources) {
        BulkRequest bulkRequest = new BulkRequest();
        sources.forEach(item -> bulkRequest.add(new IndexRequest(index, type).source(item)));
        return bulkInsert(bulkRequest);
    }

    /**
     * bulkInsert
     *
     * @param bulkRequest instance of BulkRequest
     * @return Array of doc id
     */
    public String[] bulkInsert(BulkRequest bulkRequest) {
        try {
            BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            String[] restStatus = new String[bulkResponse.getItems().length];
            int i = 0;
            for (BulkItemResponse item : bulkResponse) {
                restStatus[i] = (RestStatus.CREATED == item.status() ? item.getId() : null);
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
     * @param index  index name
     * @param script instance on Script
     * @param search Map of search
     * @return count of updated records
     */
    public long updateByQuery(String index, Script script, Map<String,
            Object> search) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(index);
        updateByQueryRequest.setDocTypes(ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE);
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(search);
        updateByQueryRequest.setQuery(queryBuilder);
        updateByQueryRequest.setScript(script);
        return updateByQuery(updateByQueryRequest);
    }

    /**
     * 根据search条件更新数据
     *
     * @param index  index name
     * @param type   doc type
     * @param script instance of Script
     * @param search Map of search
     * @return count of updated records
     */
    public long updateByQuery(String index, String type, Script script, Map<String,
            Object> search) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(index);
        updateByQueryRequest.setDocTypes(type);
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(search);
        updateByQueryRequest.setQuery(queryBuilder);
        updateByQueryRequest.setScript(script);
        return updateByQuery(updateByQueryRequest);
    }

    /**
     * 根据search条件更新数据
     *
     * @param index  index name
     * @param doc    Map of target data
     * @param search Map of search
     * @return count of updated records
     */
    public long updateByQuery(String index, Map<String, Object> doc, Map<String,
            Object> search) {
        Settings.Builder settingsBuilder = Settings.builder();
        doc.forEach((key, value) -> buildSettingsElement(settingsBuilder, key));
        Settings settings = settingsBuilder.build();
        String source = settings.toDelimitedString(';');
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, source
                , doc);
        return updateByQuery(index, ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE, script, search);
    }

    /**
     * 根据search条件更新数据
     *
     * @param index  index name
     * @param type   doc type
     * @param doc    Map of target data
     * @param search Map of search data
     * @return count of updated records
     */
    public long updateByQuery(String index, String type, Map<String, Object> doc, Map<String,
            Object> search) {
        Settings.Builder settingsBuilder = Settings.builder();
        doc.forEach((key, value) -> buildSettingsElement(settingsBuilder, key));
        Settings settings = settingsBuilder.build();
        String source = settings.toDelimitedString(';');
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, source
                , doc);
        return updateByQuery(index, type, script, search);
    }

    /**
     * updateByQuery
     *
     * @param updateByQueryRequest instance of UpdateByQueryRequest
     * @return count of updated records
     */
    public long updateByQuery(UpdateByQueryRequest updateByQueryRequest) {
        try {
            updateByQueryRequest.setMaxRetries(MAX_RETRY_TIMES);
            BulkByScrollResponse bulkResponse =
                    restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
            return bulkResponse.getUpdated();
        } catch (IOException | ElasticsearchStatusException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return 0L;
    }

    /**
     * 构建search参数
     *
     * @param builder instance of Settings.Builder
     * @param key     key
     */
    public static void buildSettingsElement(Settings.Builder builder, String key) {
        builder.put("ctx._source."+ key, "params."+ key);
    }

    /**
     * 更具search条件删除数据
     *
     * @param index  index name
     * @param search Map of search
     * @return count of deleted records
     */
    public long deleteByQuery(String index, Map<String, Object> search) {
        if (null == search|| search.isEmpty()) {
            throw new AssertionError();
        }
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
        deleteByQueryRequest.setDocTypes(ElasticSearchIndicesUtils.DEFAULT_INDEX_TYPE);
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(search);
        deleteByQueryRequest.setQuery(queryBuilder);
        return deleteByQuery(deleteByQueryRequest);
    }

    /**
     * 更具search条件删除数据
     *
     * @param index  index name
     * @param type   doc type
     * @param search Map of search
     * @return count of deleted records
     */
    public long deleteByQuery(String index, String type, Map<String, Object> search) {
        if (null == search|| search.isEmpty()) {
            throw new AssertionError();
        }
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(index);
        deleteByQueryRequest.setDocTypes(type);
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(search);
        deleteByQueryRequest.setQuery(queryBuilder);
        return deleteByQuery(deleteByQueryRequest);
    }

    /**
     * deleteByQuery
     *
     * @param deleteByQueryRequest instance of DeleteByQueryRequest
     * @return count of deleted records
     */
    public long deleteByQuery(DeleteByQueryRequest deleteByQueryRequest) {
        deleteByQueryRequest.setMaxRetries(MAX_RETRY_TIMES);
        try {
            BulkByScrollResponse bulkResponse =
                    restHighLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
            return bulkResponse.getDeleted();
        } catch (IOException | ElasticsearchStatusException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return 0L;
    }

}