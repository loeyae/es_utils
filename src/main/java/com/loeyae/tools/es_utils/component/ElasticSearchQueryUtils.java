package com.loeyae.tools.es_utils.component;


import com.loeyae.tools.es_utils.common.ElasticSearchAggregationBuilder;
import com.loeyae.tools.es_utils.common.ElasticSearchQueryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

import static com.alibaba.fastjson.JSON.toJSONString;

/**
 * elastic search query.
 *
 * @date 2020-02-12
 * @version 1.0
 * @author zhangyi<loeyae@gmail.com>
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
    static public class Result {

        private SearchResponse searchResponse;

        private String scrollId;

        private long total;

        private long count;

        private List<Map<String, Object>> source;

        private Map<String, Aggregation> aggregations;

        public void init(SearchResponse searchResponse) {
            this.searchResponse = searchResponse;
            scrollId = searchResponse.getScrollId();
            total = searchResponse.getHits().getTotalHits();
            count = searchResponse.getHits().getHits().length;
            source = parseSource();
            aggregations = parseAggregations();
        }

        /**
         * parseSource
         *
         * @return List of data
         */
        public List<Map<String, Object>> parseSource() {
            List<Map<String, Object>> result =
                    new ArrayList<>(searchResponse.getHits().getHits().length);
            this.searchResponse.getHits().iterator().forEachRemaining(
                    item -> result.add(item.getSourceAsMap()));
            return result;
        }

        /**
         * parseAggregations
         *
         * @return Map of Aggregation
         */
        public Map<String, Aggregation> parseAggregations() {
            if (null != searchResponse.getAggregations()) {
                return searchResponse.getAggregations().asMap();
            }
            return null;
        }

        /**
         * Iterator
         *
         * @return instance of Iterator
         */
        public Iterator<Map<String, Object>> iterator() {
            return source.iterator();
        }

        /**
         * getScrollId
         *
         * @return scroll id
         */
        public String getScrollId() {
            return scrollId;
        }

        /**
         * get Total
         *
         * @return total
         */
        public long getTotal() {
            return total;
        }

        /**
         * getCount
         *
         * @return count
         */
        public long getCount() {
            return count;
        }

        /**
         * getSource
         *
         * @return List of data
         */
        public List<Map<String, Object>> getSource() {
            return source;
        }

        /**
         * getSearchResponse
         *
         * @return instance of SearchResponse
         */
        public SearchResponse getSearchResponse() {
            return searchResponse;
        }

        /**
         * getAggregations
         *
         * @return Map of Aggregation
         */
        public Map<String, Aggregation> getAggregations() {
            return aggregations;
        }

        @Override
        public String toString() {
            Map<String, Object> jsonMap = new HashMap<>(5);
            jsonMap.put("scrollId", scrollId);
            jsonMap.put("total", total);
            jsonMap.put("count", count);
            jsonMap.put("source", source);
            jsonMap.put("aggregations", aggregations);
            return toJSONString(jsonMap);
        }

    }

    /**
     * result
     *
     * @param searchResponse instance of SearchResponse
     * @return instance of Result
     */
    public static Result result(SearchResponse searchResponse) {
        Result result = new Result();
        result.init(searchResponse);
        return result;
    }

    /**
     * search
     *
     * @param index         index name
     * @param jsonString    Json string of query
     * @param size          size
     * @param from          start
     * @param sort          sort setting
     * @param includeFields include fields
     * @param excludeFields exclude fields
     * @return instance of SearchResponse
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
     * @param index         index name
     * @param query         List of query
     * @param size          size
     * @param from          start
     * @param sort          sort setting
     * @param includeFields include fields
     * @param excludeFields exclude fields
     * @return instance of SearchResponse
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
     * @param index         index name
     * @param search        Map of search
     * @param size          size
     * @param from          start
     * @param sort          sort setting
     * @param includeFields include fields
     * @param excludeFields exclude fields
     * @return instance of SearchResponse
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
     * @param index         index name
     * @param queryBuilder  instance of QueryBuilder
     * @param size          size
     * @param from          start
     * @param sort          sort setting
     * @param includeFields include fields
     * @param excludeFields exclude fields
     * @return instance of SearchResponse
     */
    public SearchResponse search(String index, QueryBuilder queryBuilder, int size, int from,
                                 Map<String, Integer>sort, String[] includeFields,
                                 String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, queryBuilder, size, from,
                QUERY_TIME_VALUE_SECONDS_NULL, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * search
     *
     * @param index            index name
     * @param jsonString       Json string of searhc
     * @param size             size
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchResponse
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
     * @param index            index name
     * @param query            List of search
     * @param size             size
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchResponse
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
     * @param index            index name
     * @param search           Map of search
     * @param size             size
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchResponse
     */
    public SearchResponse search(String index, Map<String, Object>search, int size,
                                 long timeValueSeconds, Map<String, Integer>sort,
                                 String[] includeFields, String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, search, size, QUERY_FORM_NULL,
                timeValueSeconds, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * search
     *
     * @param index            index name
     * @param queryBuilder     instance of QueryBuilder
     * @param size             size
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchResponse
     */
    public SearchResponse search(String index, QueryBuilder queryBuilder, int size,
                                 long timeValueSeconds, Map<String, Integer>sort,
                                 String[] includeFields, String[] excludeFields) {
        SearchRequest searchRequest = buildRequest(index, queryBuilder, size, QUERY_FORM_NULL,
                timeValueSeconds, sort, includeFields, excludeFields);
        return query(searchRequest);
    }

    /**
     * scroll
     *
     * @param scrollId         scroll id
     * @param timeValueSeconds timeout seconds
     * @return instance of SearchResponse
     */
    public SearchResponse scroll(String scrollId, long timeValueSeconds) {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(TimeValue.timeValueSeconds(timeValueSeconds));
        return query(scrollRequest);
    }

    /**
     * clear scroll request
     *
     * @param scrollId scroll id
     * @return instance of ClearScrollResponse
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
     * 聚合
     *
     * @param indexName           index name
     * @param aggregationBuilders List of AggregationBuilder's instance
     * @param query               List of search
     * @return instance of SearchResponse
     */
    public SearchResponse aggregations(String indexName,
                                       List<AggregationBuilder> aggregationBuilders,
                                       List<Map<String, Object>>query) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(0);
        sourceBuilder.query(ElasticSearchQueryBuilder.build(query));
        aggregationBuilders.forEach(sourceBuilder::aggregation);
        searchRequest.source(sourceBuilder);
        return query(searchRequest);
    }

    /**
     * 聚合
     *
     * @param indexName    index name
     * @param aggregations List of aggregation
     * @param query        Map of search
     * @return instance of SearchResponse
     */
    public SearchResponse aggregations(String indexName,
                                       List<?> aggregations,
                                       Map<String, Object>query) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(0);
        sourceBuilder.query(ElasticSearchQueryBuilder.build(query));
        List<AggregationBuilder> aggregationBuilderList = new ArrayList<>();

        aggregations.forEach(item -> {
            if (item instanceof AggregationBuilder) {
                aggregationBuilderList.add((AggregationBuilder) item);
            } else {
                aggregationBuilderList.addAll(ElasticSearchAggregationBuilder.build(item));
            }
        });
        aggregationBuilderList.forEach(sourceBuilder::aggregation);
        searchRequest.source(sourceBuilder);
        return query(searchRequest);
    }

    /**
     * 聚合
     *
     * @param indexName    index name
     * @param aggregations Map of aggregation
     * @param query        Map of search
     * @return instance of SearchResponse
     */
    public SearchResponse aggregations(String indexName,
                                       Map<String, Object> aggregations,
                                       Map<String, Object>query) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(0);
        sourceBuilder.query(ElasticSearchQueryBuilder.build(query));
        List<AggregationBuilder> aggregationBuilderList =
                ElasticSearchAggregationBuilder.build(aggregations);
        aggregationBuilderList.forEach(sourceBuilder::aggregation);
        searchRequest.source(sourceBuilder);
        return query(searchRequest);
    }

    /**
     * 聚合
     *
     * @param indexName           index name
     * @param aggregationBuilders List of AggregationBuilder's instance
     * @param jsonString          Json string of search
     * @return instance of SearchResponse
     */
    public SearchResponse aggregations(String indexName,
                                       List<AggregationBuilder> aggregationBuilders,
                                       String jsonString) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(0);
        sourceBuilder.query(ElasticSearchQueryBuilder.build(jsonString));
        aggregationBuilders.forEach(sourceBuilder::aggregation);
        searchRequest.source(sourceBuilder);
        return query(searchRequest);
    }


    /**
     * 聚合
     *
     * @param indexName         index name
     * @param aggregationString Json string of aggregation
     * @param queryString       Json string of search
     * @return instance of SearchResponse
     */
    public SearchResponse aggregations(String indexName, String aggregationString,
                                       String queryString) {
        List<AggregationBuilder> aggregationBuilderList =
                ElasticSearchAggregationBuilder.build(aggregationString);
        return aggregations(indexName, aggregationBuilderList, queryString);
    }

    /**
     * 聚合
     *
     * @param indexName           index name
     * @param aggregationBuilders List of AggregationBuilder's instance
     * @return instance of SearchResponse
     */
    public SearchResponse aggregations(String indexName,
                                       List<AggregationBuilder> aggregationBuilders) {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(0);
        aggregationBuilders.forEach(sourceBuilder::aggregation);
        searchRequest.source(sourceBuilder);
        return query(searchRequest);
    }


    /**
     * 聚合
     *
     * @param indexName    index name
     * @param aggregations Json string of aggregation
     * @return instance of SearchResponse
     */
    public SearchResponse aggregations(String indexName, String aggregations) {
        List<AggregationBuilder> aggregationBuilderList =
                ElasticSearchAggregationBuilder.build(aggregations);
        return aggregations(indexName, aggregationBuilderList);
    }

    /**
     * 查询
     *
     * @param searchRequest instance of SearchRequest
     * @return instance of SearchResponse
     */
    public SearchResponse query(SearchRequest searchRequest) {
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.search(searchRequest,
                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return searchResponse;
    }

    /**
     * Scroll查询
     *
     * @param searchScrollRequest instance of SearchScrollRequest
     * @return instance of SearchResponse
     */
    public SearchResponse query(SearchScrollRequest searchScrollRequest) {
        SearchResponse searchResponse = null;
        try {
            searchResponse = restHighLevelClient.scroll(searchScrollRequest,
                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error(DEFAULT_ERROR_MSG, e);
        }
        return searchResponse;
    }

    /**
     * 构建SearchRequest
     *
     * @param index            index name
     * @param query            Map of search
     * @param size             size
     * @param from             start
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchRequest
     */
    protected SearchRequest buildRequest(String index, Map<String, Object> query, int size, int from,
                                         long timeValueSeconds, Map<String, Integer> sort,
                                         String[] includeFields, String[] excludeFields) {
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(query);
        return buildRequest(index, queryBuilder, size, from, timeValueSeconds, sort, includeFields,
                excludeFields);
    }

    /**
     * 构建SearchRequest
     *
     * @param index            index name
     * @param query            List of search
     * @param size             size
     * @param from             start
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchRequest
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
     * @param index            index name
     * @param query            Json string of search
     * @param size             size
     * @param from             start
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchRequest
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
     * @param index            index name
     * @param query            instance of QueryBuilder
     * @param size             size
     * @param from             start
     * @param timeValueSeconds scroll timeout seconds
     * @param sort             sort setting
     * @param includeFields    include fields
     * @param excludeFields    exclude fields
     * @return instance of SearchRequest
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
                fieldSortBuilder.order(DEFAULT_SORT_TYPE);
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