package com.loeyae.tools.es_utils.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONPath;
import org.elasticsearch.index.query.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertTrue(JSONPath.contains(json, "$.bool.must"));
        assertFalse(JSONPath.contains(json, "$.bool.must_not"));
        assertFalse(JSONPath.contains(json, "$.bool.should"));
        List<Map> list = new ArrayList<>();
        list.add(map);
        QueryBuilder queryBuilder1 = ElasticSearchQueryBuilder.build(map);
        JSON json1 = JSON.parseObject(queryBuilder1.toString());
        assertTrue(queryBuilder1 instanceof BoolQueryBuilder);
        assertTrue(JSONPath.contains(json1, "$.bool"));
        assertTrue(JSONPath.contains(json1, "$.bool.must"));
        assertFalse(JSONPath.contains(json1, "$.bool.must_not"));
        assertFalse(JSONPath.contains(json1, "$.bool.should"));
        Map<String, Object> map1 = new HashMap<>();
        map1.put(TypeQueryBuilder.NAME, new HashMap<Object, Object>(){{
            put(TypeQueryBuilder.NAME_FIELD.getPreferredName(), "id");
            put("value", "int");
        }});
        QueryBuilder queryBuilder2 = ElasticSearchQueryBuilder.build(map1);
        JSON json2 = JSON.parseObject(queryBuilder2.toString());
        assertTrue(queryBuilder2 instanceof TypeQueryBuilder);
        assertTrue(JSONPath.contains(json2, "$.type"));
        assertTrue(JSONPath.contains(json2, "$.type.value"));
        Map<String, Object> map2 = new HashMap<>();
        Map<String, Object> query = new HashMap<>();
        query.put("name", "test");
        map2.put(TermQueryBuilder.NAME, query);
        QueryBuilder queryBuilder3 = ElasticSearchQueryBuilder.build(map2);
        assertTrue(queryBuilder3 instanceof TermQueryBuilder);
        JSON json3 = JSON.parseObject(queryBuilder3.toString());
        assertTrue(JSONPath.contains(json3, "$.term"));
        assertTrue(JSONPath.contains(json3, "$.term.name"));
        assertTrue(JSONPath.contains(json3, "$.term.name.value"));
        List<Map<String, Object>> list1 = new ArrayList<>();
        Map<String, Object> map3 = new HashMap<>();
        Map<String, Object> query2 = new HashMap<>();
        query2.put("name", "test");
        map3.put(TermQueryBuilder.NAME, query2);
        Map<String, Object> map4 = new HashMap<>();
        Map<String, Object> query3 = new HashMap<>();
        query3.put("title", "test");
        map4.put(TermQueryBuilder.NAME, query3);
        list1.add(map3);
        list1.add(map4);
        QueryBuilder queryBuilder4 = ElasticSearchQueryBuilder.build(list1);
        JSON json4 = JSON.parseObject(queryBuilder4.toString());
        assertTrue(queryBuilder4 instanceof BoolQueryBuilder);
        assertTrue(JSONPath.contains(json4, "$.bool"));
        assertTrue(JSONPath.contains(json4, "$.bool.must"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.0.term"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.0.term.name"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.1.term"));
        assertTrue(JSONPath.contains(json4, "$.bool.must.1.term.title"));
        String jsonString = "{'must': [{'term': {'name': 'test'}}]}";
        QueryBuilder queryBuilder5 = ElasticSearchQueryBuilder.build(jsonString);
        assertTrue(queryBuilder5 instanceof BoolQueryBuilder);
        JSON json5 = JSON.parseObject(queryBuilder5.toString());
        assertTrue(JSONPath.contains(json5, "$.bool"));
        assertTrue(JSONPath.contains(json5, "$.bool.must"));
        assertTrue(JSONPath.contains(json5, "$.bool.must.0.term"));
        assertTrue(JSONPath.contains(json5, "$.bool.must.0.term.name"));
        Map<String, Object> map5 = null;
        QueryBuilder queryBuilder6 = ElasticSearchQueryBuilder.build(map5);
        assertTrue(queryBuilder6 instanceof MatchAllQueryBuilder);
        List<Map<String, Object>> list2 = null;
        QueryBuilder queryBuilder7 = ElasticSearchQueryBuilder.build(list2);
        assertTrue(queryBuilder7 instanceof MatchAllQueryBuilder);
        QueryBuilder queryBuilder8 = ElasticSearchQueryBuilder.build((String)null);
        assertTrue(queryBuilder8 instanceof MatchAllQueryBuilder);

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
        params2.put(TermQueryBuilder.NAME, "test");
        assertTrue(ElasticSearchQueryBuilder.containsJoinKey(params2));
    }
}