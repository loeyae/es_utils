package com.loeyae.tools.es_utils.component;


import com.alibaba.fastjson.JSONObject;
import com.loeyae.tools.es_utils.common.ElasticSearchQueryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * elastic search query.
 *
 * @date: 2020-02-12
 * @version: 1.0
 * @author: zhangyi07@beyondsoft.com
 */
@Slf4j
@Component
public class ElasticSearchQueryUtils {

    public static final int QUERY_FORM_NULL = -1;

    public static final long QUERY_TIME_VALUE_SECONDS_NULL  = 0L;

    private static final String DEFAULT_ERROR_MSG = "ES Error: ";

    private static final SortOrder DEFAULT_SORT_TYPE = SortOrder.DESC;

    @Autowired
    RestHighLevelClient restHighLevelClient;

    /**
     * searchResponse 解析
     */
    static class Result {

        private SearchResponse searchResponse;

        private String scrollId;

        private long total;

        private long count;

        private List<Map<String, Object>> source;

        public void init(SearchResponse searchResponse) {
            this.searchResponse = searchResponse;
            scrollId = searchResponse.getScrollId();
            total = searchResponse.getHits().getTotalHits();
            count = searchResponse.getHits().getHits().length;
            source = parseSource();
        }

        public List<Map<String, Object>> parseSource() {
            List<Map<String, Object>> result =new ArrayList<>(searchResponse.getHits().getHits().length);
            this.searchResponse.getHits().iterator().forEachRemaining(item -> {
                result.add(item.getSourceAsMap());
            });
            return result;
        }

        /**
         * Iterator
         *
         * @return
         */
        public Iterator<Map<String, Object>> iterator() {
            return source.iterator();
        }

        public String getScrollId() {
            return scrollId;
        }

        public long getTotal() {
            return total;
        }

        public long getCount() {
            return count;
        }

        public List<Map<String, Object>> getSource() {
            return source;
        }

        public SearchResponse getSearchResponse() {
            return searchResponse;
        }

        @Override
        public String toString() {
            Map<String, Object> jsonMap = new HashMap<String, Object>(){{
                put("scrollId", scrollId);
                put("total", total);
                put("source", source);
            }};
            return JSONObject.toJSONString(jsonMap);
        }

    }

    /**
     *
     * @param searchResponse
     * @return
     */
    static public Result result(SearchResponse searchResponse) {
        Result result = new Result();
        result.init(searchResponse);
        return result;
    }

    /**
     * search
     *
     * @param index
     * @param jsonString
     * @param size
     * @param from
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    public SearchResponse search(String index, String jsonString, int size, int from,
                                 Map<String, Integer>sort, String[] includeFields,
                                 String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, jsonString, size, from,
                QUERY_TIME_VALUE_SECONDS_NULL, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * search
     *
     * @param index
     * @param query
     * @param size
     * @param from
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    public SearchResponse search(String index, List<Map<String, Object>> query, int size, int from,
                                 Map<String, Integer>sort, String[] includeFields,
                                 String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, query, size, from,
                QUERY_TIME_VALUE_SECONDS_NULL, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * search
     *
     * @param index
     * @param search
     * @param size
     * @param from
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    public SearchResponse search(String index, Map<String, Object>search, int size, int from,
                                 Map<String, Integer>sort, String[] includeFields,
                                 String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, search, size, from,
                QUERY_TIME_VALUE_SECONDS_NULL, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * search
     *
     * @param index
     * @param jsonString
     * @param size
     * @param timeValueSeconds
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    public SearchResponse search(String index, String jsonString, int size, long timeValueSeconds,
                                 Map<String, Integer>sort, String[] includeFields,
                                 String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, jsonString, size, QUERY_FORM_NULL,
                timeValueSeconds, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * search
     *
     * @param index
     * @param query
     * @param size
     * @param timeValueSeconds
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    public SearchResponse search(String index, List<Map<String, Object>> query, int size,
                                 long timeValueSeconds, Map<String, Integer>sort,
                                 String[] includeFields, String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, query, size, QUERY_FORM_NULL,
                timeValueSeconds, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * search
     *
     * @param index
     * @param search
     * @param size
     * @param timeValueSeconds
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    public SearchResponse search(String index, Map<String, Object>search, int size,
                                 long timeValueSeconds, Map<String, Integer>sort,
                                 String[] includeFields, String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, search, size, QUERY_FORM_NULL,
                timeValueSeconds, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * scroll
     *
     * @param scrollId
     * @param timeValueSeconds
     * @return
     */
    public SearchResponse scroll(String scrollId, long timeValueSeconds) {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueSeconds(timeValueSeconds));
        return query(scrollRequest);
    }

    /**
     * clear scroll request
     *
     * @param scrollId
     * @return
     */
    public ClearScrollResponse clearScroll(String... scrollId) {
        ClearScrollRequest scrollRequest = new ClearScrollRequest();
        scrollRequest.setScrollIds(Arrays.asList(scrollId));
        try {
            return restHighLevelClient.clearScroll(scrollRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return null;
    }

    /**
     * 查询
     *
     * @param searchRequest
     * @return
     */
    public SearchResponse query(SearchRequest searchRequest) {
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest,
                    RequestOptions.DEFAULT);
            return searchResponse;
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return null;
    }

    /**
     * Scroll查询
     *
     * @param searchScrollRequest
     * @return
     */
    public SearchResponse query(SearchScrollRequest searchScrollRequest) {
        try {
            SearchResponse searchResponse = restHighLevelClient.scroll(searchScrollRequest,
                    RequestOptions.DEFAULT);
            return searchResponse;
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return null;
    }

    /**
     * 构建SearchRequest
     *
     * @param index
     * @param query
     * @param size
     * @param from
     * @param timeValueSeconds
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    protected SearchRequest buildRequest(String index, Map<String, Object> query, int size,
                                         int from,
                                         long timeValueSeconds, Map<String, Integer> sort,
                                         String[] includeFields, String[] excludeFields) {
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(query);
        return buildRequest(index, queryBuilder, size, from, timeValueSeconds, sort, includeFields,
                excludeFields);
    }

    /**
     * 构建SearchRequest
     *
     * @param index
     * @param query
     * @param size
     * @param from
     * @param timeValueSeconds
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    protected SearchRequest buildRequest(String index, List<Map<String, Object>> query, int size,
                                         int from,
                                         long timeValueSeconds, Map<String, Integer> sort,
                                         String[] includeFields, String[] excludeFields) {
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(query);
        return buildRequest(index, queryBuilder, size, from, timeValueSeconds, sort, includeFields,
                excludeFields);
    }

    /**
     * 构建SearchRequest
     *
     * @param index
     * @param query
     * @param size
     * @param from
     * @param timeValueSeconds
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    protected SearchRequest buildRequest(String index, String query, int size, int from,
                                         long timeValueSeconds, Map<String, Integer> sort,
                                         String[] includeFields, String[] excludeFields) {
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(query);
        return buildRequest(index, queryBuilder, size, from, timeValueSeconds, sort, includeFields,
                excludeFields);
    }

    /**
     * 构建SearchRequest
     *
     * @param index
     * @param query
     * @param size
     * @param from
     * @param timeValueSeconds
     * @param sort
     * @param includeFields
     * @param excludeFields
     * @return
     */
    protected SearchRequest buildRequest(String index, QueryBuilder query, int size, int from,
                                         long timeValueSeconds, Map<String, Integer> sort,
                                         String[] includeFields, String[] excludeFields) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        SearchSourceBuilder searchSource = new SearchSourceBuilder();
        searchSource.query(query);
        searchSource.size(size);
        if (timeValueSeconds <= 0 && from >= 0) {
            searchSource.from(from);
        }
        if (null != sort) {
            Map.Entry<String, Integer> c = sort.entrySet().iterator().next();
            FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(c.getKey());
            if (c.getValue() > 0) {
                fieldSortBuilder.order(SortOrder.ASC);
            } else {
                fieldSortBuilder.order(SortOrder.DESC);
            }
            searchSource.sort(fieldSortBuilder);
        }
        if (null != includeFields) {
            searchSource.fetchSource(includeFields, null);
        } else if (null != excludeFields) {
            searchSource.fetchSource(null, excludeFields);
        }
        searchRequest.source(searchSource);
        if (timeValueSeconds > 0) {
            searchRequest.scroll(TimeValue.timeValueSeconds(timeValueSeconds));
        }
        return searchRequest;
    }

}