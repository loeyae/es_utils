package com.loeyae.tools.es_utils.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONPath;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TypeQueryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ElasticSearchQueryBuilderTest {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testBuild() {
        Map<String, Object> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        QueryBuilder queryBuilder = ElasticSearchQueryBuilder.build(map);
        JSON json = JSON.parseObject(queryBuilder.toString());
        assertTrue(queryBuilder instanceof BoolQueryBuilder);
        assertTrue(JSONPath.contains(json, "$.bool"));
        assertFalse(JSONPath.contains(json, "$.bool.must"));
        assertFalse(JSONPath.contains(json, "$.bool.must_not"));
        assertFalse(JSONPath.contains(json, "$.bool.should"));
        List<Map> list = new ArrayList<>();
        list.add(map);
        QueryBuilder queryBuilder1 = ElasticSearchQueryBuilder.build(map);
        JSON json1 = JSON.parseObject(queryBuilder1.toString());
        assertTrue(queryBuilder1 instanceof BoolQueryBuilder);
        assertTrue(JSONPath.contains(json1, "$.bool"));
        assertFalse(JSONPath.contains(json1, "$.bool.must"));
        assertFalse(JSONPath.contains(json1, "$.bool.must_not"));
        assertFalse(JSONPath.contains(json1, "$.bool.should"));
        Map<String, Object> map1 = new HashMap<>();
        map1.put(ElasticSearchQueryFactory.QUERY_TYPE_TYPE, "int");
        QueryBuilder queryBuilder2 = ElasticSearchQueryBuilder.build(map1);
        JSON json2 = JSON.parseObject(queryBuilder2.toString());
        assertTrue(queryBuilder2 instanceof TypeQueryBuilder);
        assertTrue(JSONPath.contains(json2, "$.type"));
        assertTrue(JSONPath.contains(json2, "$.type.value"));
        Map<String, Object> map2 = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        map2.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query);
        QueryBuilder queryBuilder3 = ElasticSearchQueryBuilder.build(map2);
        assertTrue(queryBuilder3 instanceof TermQueryBuilder);
        JSON json3 = JSON.parseObject(queryBuilder3.toString());
        assertTrue(JSONPath.contains(json3, "$.term"));
        assertTrue(JSONPath.contains(json3, "$.term.name"));
        assertTrue(JSONPath.contains(json3, "$.term.name.value"));
        List<Map<String, Object>> list1 = new ArrayList<>();
        Map<String, Object> map3 = new HashMap<>();
        Map<String, Object> query2 = new HashMap<>();
        query2.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        query2.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        map3.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query2);
        Map<String, Object> map4 = new HashMap<>();
        Map<String, Object> query3 = new HashMap<>();
        query3.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "title");
        query3.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        map4.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query3);
        list1.add(map3);
        list1.add(map4);
        QueryBuilder queryBuilder4 = ElasticSearchQueryBuilder.build(list1);
        JSON json4 = JSON.parseObject(queryBuilder4.toString());
        assertTrue(JSONPath.contains(json4, "$.bool"));
        assertTrue(JSONPath.contains(json4, "$.bool.must"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.0.term"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.0.term.name"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.1.term"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.1.term.title"));
        String jsonString = "{'must': [{'term': {'field':'name', 'query':'test'}}]}";
        QueryBuilder queryBuilder5 = ElasticSearchQueryBuilder.build(jsonString);
        JSON json5 = JSON.parseObject(queryBuilder5.toString());
        assertTrue(JSONPath.contains(json5, "$.bool"));
        assertTrue(JSONPath.contains(json5, "$.bool.must"));
        assertTrue(JSONPath.contains(json5, "$.bool.must.0.term"));
        assertTrue(JSONPath.contains(json5, "$.bool.must.0.term.name"));
    }

    @Test
    void testBuildMultiQueryBuilder() {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        List<Map> mapList = new ArrayList<>();
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        params.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query);
        mapList.add(params);
        Map<String, Object> params1 = new HashMap<>();
        Map<String, Object> query1 = new HashMap<>();
        query1.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        query1.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        params1.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query1);
        mapList.add(params1);
        Map<String, Object> listMap = new HashMap<>();
        listMap.put(ElasticSearchQueryBuilder.JOIN_TYPE_MUST, mapList);
        ElasticSearchQueryBuilder.buildMultiQueryBuilder(queryBuilder, listMap);
        String queryString = queryBuilder.toString();
        JSON json = JSON.parseObject(queryString);
        assertTrue(JSONPath.contains(json, "$.bool"));
        assertTrue(JSONPath.contains(json, "$.bool.must"));
        assertTrue(JSONPath.contains(json, "$.bool.must.0.term"));
        assertTrue(JSONPath.contains(json, "$.bool.must.1.term"));
    }

    @Test
    void testBuildMustQuery() {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        params.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query);
        ElasticSearchQueryBuilder.buildMustQuery(queryBuilder, params);
        String queryString = queryBuilder.toString();
        JSON json = JSON.parseObject(queryString);
        assertTrue(JSONPath.contains(json, "$.bool"));
        assertTrue(JSONPath.contains(json, "$.bool.must"));
        assertTrue(JSONPath.contains(json, "$.bool.must.0.term"));
    }

    @Test
    void testBuildMustNotQuery() {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        params.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query);
        ElasticSearchQueryBuilder.buildMustNotQuery(queryBuilder, params);
        String queryString = queryBuilder.toString();
        JSON json = JSON.parseObject(queryString);
        assertTrue(JSONPath.contains(json, "$.bool"));
        assertTrue(JSONPath.contains(json, "$.bool.must_not"));
        assertTrue(JSONPath.contains(json, "$.bool.must_not.0.term"));
    }

    @Test
    void testBuildShouldQuery() {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        query.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        params.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, query);
        ElasticSearchQueryBuilder.buildShouldQuery(queryBuilder, params);
        String queryString = queryBuilder.toString();
        JSON json = JSON.parseObject(queryString);
        assertTrue(JSONPath.contains(json, "$.bool"));
        assertTrue(JSONPath.contains(json, "$.bool.should"));
        assertTrue(JSONPath.contains(json, "$.bool.should.0.term"));
    }

    @Test
    void testContainsJoinKey() {
        Map<String, Object> params = new HashMap<>();
        params.put(ElasticSearchQueryBuilder.JOIN_TYPE_MUST, "test");
        assertTrue(ElasticSearchQueryBuilder.containsJoinKey(params));
        Map<String, Object> params1 = new HashMap<>();
        params1.put("test", "test");
        assertFalse(ElasticSearchQueryBuilder.containsJoinKey(params1));
        Map<String, Object> params2 = new HashMap<>();
        params2.put(ElasticSearchQueryBuilder.JOIN_TYPE_MUST, "test");
        params2.put(ElasticSearchQueryFactory.QUERY_TYPE_TERM, "test");
        assertTrue(ElasticSearchQueryBuilder.containsJoinKey(params2));
    }
}