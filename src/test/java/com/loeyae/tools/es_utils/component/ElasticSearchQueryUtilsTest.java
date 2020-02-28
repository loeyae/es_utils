package com.loeyae.tools.es_utils.component;

import com.loeyae.tools.es_utils.common.ElasticSearchQueryFactory;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
class ElasticSearchQueryUtilsTest {

    private static final String indexName = "zy-sample";

    @Autowired
    ElasticSearchQueryUtils utils;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testSearchByNull() {
        SearchResponse searchResponse = utils.search(indexName, (String )null, 2,
                ElasticSearchQueryUtils.QUERY_FORM_NULL, null, null, null);
        assertNotNull(searchResponse);
        assertTrue(RestStatus.OK == searchResponse.status());
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertNull(result.getScrollId());
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getSource().size() > 0);
        SearchResponse searchResponse1 = utils.search(indexName, (String)null, 2, 0,
                new HashMap<String, Integer>(){{
                    put("id", 1);
        }}, null, null);
        assertNotNull(searchResponse1);
        assertTrue(RestStatus.OK == searchResponse1.status());
        ElasticSearchQueryUtils.Result result1 = ElasticSearchQueryUtils.result(searchResponse1);
        assertNull(result1.getScrollId());
        assertTrue(result1.getTotal() > 0);
        assertTrue(result1.getSource().size() > 0);
        assertFalse(result.toString().equals(result1.toString()));
        SearchResponse searchResponse2 = utils.search(indexName, (String)null, 2, 0,
                new HashMap<String, Integer>(){{
            put("id", -1);
        }}, null, null);
        assertNotNull(searchResponse2);
        assertTrue(RestStatus.OK == searchResponse2.status());
        ElasticSearchQueryUtils.Result result2 = ElasticSearchQueryUtils.result(searchResponse2);
        assertNull(result2.getScrollId());
        assertTrue(result2.getTotal() > 0);
        assertTrue(result2.getSource().size() > 0);
        assertFalse(result.toString().equals(result2.toString()));
    }

    @Test
    void  testSearchByNullWithFields() {
        SearchResponse searchResponse = utils.search(indexName, (String )null, 1,
                ElasticSearchQueryUtils.QUERY_FORM_NULL, null, new String[]{"id", "amount"}, null);
        assertNotNull(searchResponse);
        assertTrue(RestStatus.OK == searchResponse.status());
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertNull(result.getScrollId());
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getSource().size() > 0);
        assertTrue((result.getSource().get(0)).containsKey("id"));
        assertTrue(result.getSource().get(0).containsKey("amount"));
        assertFalse(result.getSource().get(0).containsKey("price"));
        SearchResponse searchResponse1 = utils.search(indexName, (String )null, 1,
                ElasticSearchQueryUtils.QUERY_FORM_NULL, null, null, new String[]{"amount"});
        assertNotNull(searchResponse1);
        assertTrue(RestStatus.OK == searchResponse1.status());
        ElasticSearchQueryUtils.Result result1 = ElasticSearchQueryUtils.result(searchResponse1);
        assertNull(result1.getScrollId());
        assertTrue(result1.getTotal() > 0);
        assertTrue(result1.getSource().size() > 0);
        assertTrue(result1.getSource().get(0).containsKey("id"));
        assertFalse(result1.getSource().get(0).containsKey("amount"));
        assertTrue(result1.getSource().get(0).containsKey("price"));
    }

    @Test
    void testSearchByMap() {
        Map<String, Object> search = new HashMap<String, Object>(){{
            put("id", "1");
        }};
        SearchResponse searchResponse = utils.search(indexName, search, 1,
                ElasticSearchQueryUtils.QUERY_FORM_NULL, null, null, null);
        assertNotNull(searchResponse);
        assertTrue(RestStatus.OK == searchResponse.status());
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertNull(result.getScrollId());
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getSource().size() > 0);
    }

    @Test
    void testSearchByList() {
        List<Map<String, Object>> search = new ArrayList<Map<String, Object>>(){{
            add(new HashMap<String, Object>(){{
                put("id", 1);
            }});
        }};
        SearchResponse searchResponse = utils.search(indexName, search, 1, 0, null, null, null);
        assertNotNull(searchResponse);
        assertTrue(RestStatus.OK == searchResponse.status());
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertNull(result.getScrollId());
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getSource().size() > 0);
    }

    @Test
    void testSearchByJsonString() {
        String search = "{'id': 1}";
        SearchResponse searchResponse = utils.search(indexName, search, 1, 0, null, null, null);
        assertNotNull(searchResponse);
        assertTrue(RestStatus.OK == searchResponse.status());
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertNull(result.getScrollId());
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getSource().size() > 0);
    }

    @Test
    void testScrollByNull() {
        SearchResponse searchResponse = utils.search(indexName, (String )null, 100, 60L,
                new HashMap<String, Integer>(){{
                    put("id", 1);
                }},
                null, null);
        ElasticSearchQueryUtils.Result result = null;
        Integer id = null;
        result = ElasticSearchQueryUtils.result(searchResponse);
        assertNotNull(result.getScrollId());
        assertEquals(1000000, result.getTotal());
        assertEquals(100, result.getCount());
        id = Integer.valueOf(result.getSource().get(0).get("id").toString());
        assertTrue(id > 0);
        List<String> scrollIds = new ArrayList<>();
        int i = 0;
        while (null != result.getScrollId() && i < 3) {
            scrollIds.add(result.getScrollId());
            result = ElasticSearchQueryUtils.result(utils.scroll(result.getScrollId(), 60L));
            assertEquals(1000000, result.getTotal());
            assertEquals(100, result.getCount());
            Integer nid = Integer.valueOf(result.getSource().get(0).get("id").toString());
            assertTrue(id != nid);
            id = nid;
            i++;
        }
        if (null != result.getScrollId()) {
            scrollIds.add(result.getScrollId());
        }
        String[] scrollIdArray = new String[scrollIds.size()];
        int j = 0;
        for (String item : scrollIds) {
            scrollIdArray[j] = item;
            j++;
        }
        ClearScrollResponse clearScrollResponse = utils.clearScroll(scrollIdArray);
        assertNotNull(clearScrollResponse);
        assertTrue(RestStatus.OK == clearScrollResponse.status());
    }

    @Test
    void testSearchByMultiParams() {
        Map<String, Object> search = new HashMap<String, Object>(){{
            put("id", new ArrayList<Integer>(){{
                add(null);
                add(10);
            }});
            put(ElasticSearchQueryFactory.QUERY_TYPE_QUERY_STRING,
                    new HashMap<String, Object>(){{
                        put("message", "测试");
                    }});
        }};
        SearchResponse searchResponse = utils.search(indexName, search, 100, 0,
                new HashMap<String, Integer>(){{
                    put("id", 1);
                }}, null, null);
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getCount() > 0);
        assertTrue(result.getCount() == result.getSource().size());
    }

    @Test
    void testSearchByQueryBuilder() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(new RangeQueryBuilder("id"){{
            to(10);
        }});
        boolQueryBuilder.must(new QueryStringQueryBuilder("测试").field("message"));
        SearchResponse searchResponse = utils.search(indexName, boolQueryBuilder, 100, 0,
                new HashMap<String, Integer>(){{
                    put("id", 1);
                }}, null, null);
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getCount() > 0);
        assertTrue(result.getSource().size() > 0);
        assertTrue(result.getCount() == result.getSource().size());
    }

    @Test
    void testAggregations() {
        AggregationBuilder aggregationBuilder =
                AggregationBuilders.count("count").field("id");
        SearchResponse searchResponse = utils.aggregations(indexName, aggregationBuilder);
        ParsedValueCount count = searchResponse.getAggregations().get("count");
        assertEquals(1000000, count.getValue());
        SearchResponse searchResponse1 = utils.aggregations(indexName, aggregationBuilder,
                new HashMap<String, Object>(){{
                    put("id", new ArrayList<Integer>(){{
                        add(500001);
                    }});
                }});
        ParsedValueCount count1 = searchResponse1.getAggregations().get("count");
        assertEquals(500000, count1.getValue());
        SearchResponse searchResponse2 = utils.aggregations(indexName, aggregationBuilder,
                new ArrayList<Map<String, Object>>(){{
                    add(new HashMap<String, Object>(){{
                        put("id", new ArrayList<Integer>(){{
                            add(null);
                            add(500000);
                        }});
                    }});
                }});
        ParsedValueCount count2 = searchResponse2.getAggregations().get("count");
        assertEquals(500000, count2.getValue());
        String jsonString = "{'id':[0, 500000]}";
        SearchResponse searchResponse3 = utils.aggregations(indexName, aggregationBuilder,
                jsonString);
        ParsedValueCount count3 = searchResponse3.getAggregations().get("count");
        assertEquals(500000, count3.getValue());
    }

    @Test
    void testSearchBySearchRequestWithAggregations() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(false);
        sourceBuilder.size(0);
        AggregationBuilder aggregationBuilder = AggregationBuilders.count("count").field("id");
        sourceBuilder.aggregation(aggregationBuilder);
        searchRequest.indices(indexName);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = utils.query(searchRequest);
        ParsedValueCount count = searchResponse.getAggregations().get("count");
        assertEquals(1000000, count.getValue());
    }

    @Test
    void testSearchWithAggregations() {
        
    }

}