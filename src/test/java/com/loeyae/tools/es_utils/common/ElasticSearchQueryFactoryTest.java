package com.loeyae.tools.es_utils.common;

import org.elasticsearch.index.query.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
class ElasticSearchQueryFactoryTest {

    @Test
    void build() {
        String key = ElasticSearchQueryFactory.QUERY_TYPE_TYPE;
        String param = "int";
        QueryBuilder queryBuilder = ElasticSearchQueryFactory.build(key, param);
        assertTrue(queryBuilder instanceof TypeQueryBuilder);
        String key1 = ElasticSearchQueryFactory.QUERY_TYPE_EXISTS;
        String param1 = "text";
        QueryBuilder queryBuilder1 = ElasticSearchQueryFactory.build(key1, param1);
        assertTrue(queryBuilder1 instanceof ExistsQueryBuilder);
        String key2 = ElasticSearchQueryFactory.QUERY_TYPE_TERM;
        Map<String, Object> param2 = new HashMap<>();
        param2.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        param2.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "test");
        QueryBuilder queryBuilder2 = ElasticSearchQueryFactory.build(key2, param2);
        assertTrue(queryBuilder2 instanceof TermQueryBuilder);
        String key3 = ElasticSearchQueryFactory.QUERY_TYPE_TERMS;
        Map<String, Object> param3 = new HashMap<>();
        param3.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        String[] query = new String[]{"test1", "test2"};
        param3.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, query);
        QueryBuilder queryBuilder3 = ElasticSearchQueryFactory.build(key3, param3);
        assertTrue(queryBuilder3 instanceof TermsQueryBuilder);
        String key4 = ElasticSearchQueryFactory.QUERY_TYPE_RANGE;
        Map<String, Object> param4 = new HashMap<>();
        param4.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        param4.put(ElasticSearchQueryFactory.QUERY_PARAMS_FROM, 1);
        param4.put(ElasticSearchQueryFactory.QUERY_PARAMS_TO, 10);
        QueryBuilder queryBuilder4 = ElasticSearchQueryFactory.build(key4, param4);
        assertTrue(queryBuilder4 instanceof RangeQueryBuilder);
    }

    @Test
    void buildUnaryQueryBuilder() {
        QueryBuilder queryBuilder =
                ElasticSearchQueryFactory.buildUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_EXISTS, "test");
        assertTrue(queryBuilder instanceof ExistsQueryBuilder);
        QueryBuilder queryBuilder1 =
                ElasticSearchQueryFactory.buildUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_TYPE, "int");
        assertTrue(queryBuilder1 instanceof TypeQueryBuilder);
        QueryBuilders.idsQuery();
        QueryBuilder queryBuilder2 =
                ElasticSearchQueryFactory.buildUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_IDS, 1);
        assertTrue(queryBuilder2 instanceof IdsQueryBuilder);
        QueryBuilder queryBuilder3 =
                ElasticSearchQueryFactory.buildUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_IDS, new Integer[]{1,2,3});
        assertTrue(queryBuilder3 instanceof IdsQueryBuilder);
        QueryBuilder queryBuilder4 =
                ElasticSearchQueryFactory.buildUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_QUERY_STRING,"test");
        assertTrue(queryBuilder4 instanceof QueryStringQueryBuilder);
        QueryBuilder queryBuilder5 =
                ElasticSearchQueryFactory.buildUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_SIMPLE_QUERY_STRING, "test");
        assertTrue(queryBuilder5 instanceof SimpleQueryStringBuilder);
        QueryBuilder queryBuilder6 = ElasticSearchQueryFactory.buildUnaryQueryBuilder("test",
                "text");
        assertNull(queryBuilder6);
    }

    @Test
    void buildTwoUnaryQueryBuilder() {
        Map<String, Object> params = new HashMap<>();
        params.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        params.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, "测试");
        QueryBuilder queryBuilder =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_COMMON_TERMS, params);
        assertTrue(queryBuilder instanceof CommonTermsQueryBuilder);
        QueryBuilder queryBuilder1 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_FUZZY, params);
        assertTrue(queryBuilder1 instanceof FuzzyQueryBuilder);
        QueryBuilder queryBuilder2 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_MATCH, params);
        assertTrue(queryBuilder2 instanceof  MatchQueryBuilder);
        QueryBuilder queryBuilder3 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_MATCH_PHRASE, params);
        assertTrue(queryBuilder3 instanceof  MatchPhraseQueryBuilder);
        QueryBuilder queryBuilder4 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_MATCH_PHRASE_PREFIX, params);
        assertTrue(queryBuilder4 instanceof MatchPhrasePrefixQueryBuilder);
        QueryBuilder queryBuilder5 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_PREFIX, params);
        assertTrue(queryBuilder5 instanceof PrefixQueryBuilder);
        QueryBuilder queryBuilder6 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_REGEXP, params);
        assertTrue(queryBuilder6 instanceof RegexpQueryBuilder);
        QueryBuilder queryBuilder7 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_TERM, params);
        assertTrue(queryBuilder7 instanceof TermQueryBuilder);
        QueryBuilder queryBuilder9 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_WILDCARD, params);
        assertTrue(queryBuilder9 instanceof WildcardQueryBuilder);
        params.remove(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY);
        String[] terms = new String[]{"测试1", "测试2"};
        params.put(ElasticSearchQueryFactory.QUERY_PARAMS_QUERY, terms);
        QueryBuilder queryBuilder8 =
                ElasticSearchQueryFactory.buildTwoUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_TERMS, params);
        assertTrue(queryBuilder8 instanceof TermsQueryBuilder);
    }

    @Test
    void buildTreeUnaryQueryBuilder() {
        Map<String, Object> param = new HashMap<>();
        param.put(ElasticSearchQueryFactory.QUERY_PARAMS_FIELD, "name");
        param.put(ElasticSearchQueryFactory.QUERY_PARAMS_FROM, 1);
        param.put(ElasticSearchQueryFactory.QUERY_PARAMS_TO, 10);
        QueryBuilder queryBuilder =
                ElasticSearchQueryFactory.buildTreeUnaryQueryBuilder(ElasticSearchQueryFactory.QUERY_TYPE_RANGE, param);
        assertTrue(queryBuilder instanceof  RangeQueryBuilder);
    }

    @Test
    void testKeyToMethodName() {
        String s = "test_unit_fun";
        String expected = "testUnitFunQuery";
        assertEquals(expected, ElasticSearchQueryFactory.keyToMethodName(s));
        String s1 = "test";
        String expected1 = "testQuery";
        assertEquals(expected1, ElasticSearchQueryFactory.keyToMethodName(s1));
        String s2 = "01111";
        String expected2 = "01111Query";
        assertEquals(expected2, ElasticSearchQueryFactory.keyToMethodName(s2));
        String s3 = "Test";
        String expected3 = "testQuery";
        assertEquals(expected3, ElasticSearchQueryFactory.keyToMethodName(s3));
        String s4 = "Test_App_Unit";
        String expected4 = "testAppUnitQuery";
        assertEquals(expected4, ElasticSearchQueryFactory.keyToMethodName(s4));
        String s5 = "Test_APP_2";
        String expected5 = "testApp2Query";
        assertEquals(expected5, ElasticSearchQueryFactory.keyToMethodName(s5));
    }

    @Test
    void testUcFirst() {
        String s = "test";
        String expected = "Test";
        assertEquals(expected, ElasticSearchQueryFactory.ucFirst(s));
        String s1 = "Test";
        String expected1 = "Test";
        assertEquals(expected1, ElasticSearchQueryFactory.ucFirst(s1));
        String s2 = "TEST";
        String expected2 = "TEST";
        assertEquals(expected2, ElasticSearchQueryFactory.ucFirst(s2));
        String s3 = "tEst";
        String expected3 = "TEst";
        assertEquals(expected3, ElasticSearchQueryFactory.ucFirst(s3));
    }

    @Test
    void testLcFirst() {
        String s = "test";
        String expected = "test";
        assertEquals(expected, ElasticSearchQueryFactory.lcFirst(s));
        String s1 = "Test";
        String expected1 = "test";
        assertEquals(expected1, ElasticSearchQueryFactory.lcFirst(s));
        String s2 = "1";
        String expected2 = "1";
        assertEquals(expected2, ElasticSearchQueryFactory.lcFirst(s2));
        String s3 = "TEST";
        String expected3 = "tEST";
        assertEquals(expected3, ElasticSearchQueryFactory.lcFirst(s3));
    }
}