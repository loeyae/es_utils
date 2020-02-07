package com.loeyae.tools.es_utils.common;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * elastic search query builder factory.
 *
 * @date: 2020-02-07
 * @version: 1.0
 * @author: zhangyi07@beyondsoft.com
 */
@Slf4j
public class ElasticSearchQueryFactory {

    private static final String DEFAULT_ERROR = "ES Utils Error: ";
    private static final String QUERY_PARAMS_FIELD = "field";
    private static final String QUERY_PARAMS_QUERY = "query";
    private static final String QUERY_PARAMS_LT = "lt";
    private static final String QUERY_PARAMS_LTE = "lte";
    private static final String QUERY_PARAMS_TO = "to";
    private static final String QUERY_PARAMS_GT = "gt";
    private static final String QUERY_PARAMS_GTE = "gte";
    private static final String QUERY_PARAMS_FROM = "from";
    public static final String QUERY_TYPE_MATCH = "match";
    public static final String QUERY_TYPE_MATCH_PHRASE = "match_phrase";
    public static final String QUERY_TYPE_MATCH_PHRASE_PREFIX = "match_phrase_prefix";
    public static final String QUERY_TYPE_COMMON_TERMS = "common";
    public static final String QUERY_TYPE_QUERY_STRING = "query_string";
    public static final String QUERY_TYPE_SIMPLE_QUERY_STRING = "simple_query_string";
    public static final String QUERY_TYPE_TERM = "term";
    public static final String QUERY_TYPE_TERMS = "terms";
    public static final String QUERY_TYPE_RANGE = "range";
    public static final String QUERY_TYPE_EXISTS = "exists";
    public static final String QUERY_TYPE_PREFIX = "prefix";
    public static final String QUERY_TYPE_WILDCARD = "wildcard";
    public static final String QUERY_TYPE_REGEXP = "regexp";
    public static final String QUERY_TYPE_FUZZY = "fuzzy";
    public static final String QUERY_TYPE_TYPE = "type";
    public static final String QUERY_TYPE_IDS = "ids";
    /**
     * 所有查询关键词
     */
    private static final String[] QUERY_TYPES = new String[]{
            QUERY_TYPE_COMMON_TERMS, QUERY_TYPE_EXISTS, QUERY_TYPE_FUZZY, QUERY_TYPE_IDS,
            QUERY_TYPE_MATCH, QUERY_TYPE_MATCH_PHRASE, QUERY_TYPE_MATCH_PHRASE_PREFIX,
            QUERY_TYPE_PREFIX, QUERY_TYPE_QUERY_STRING, QUERY_TYPE_RANGE, QUERY_TYPE_REGEXP,
            QUERY_TYPE_SIMPLE_QUERY_STRING, QUERY_TYPE_TERM, QUERY_TYPE_TERMS, QUERY_TYPE_TYPE,
            QUERY_TYPE_WILDCARD
    };

    /**
     * 一元关键词
     */
    private static final String[] UNARY_QUERY_TYPES = new String[]{
            QUERY_TYPE_EXISTS, QUERY_TYPE_IDS, QUERY_TYPE_QUERY_STRING,
            QUERY_TYPE_SIMPLE_QUERY_STRING, QUERY_TYPE_TYPE
    };

    /**
     * 二元关键词
     */
    private static final String[] TWO_UNARY_QUERY_TYPES = new String[]{
            QUERY_TYPE_COMMON_TERMS, QUERY_TYPE_FUZZY, QUERY_TYPE_MATCH, QUERY_TYPE_MATCH_PHRASE,
            QUERY_TYPE_MATCH_PHRASE_PREFIX, QUERY_TYPE_PREFIX, QUERY_TYPE_REGEXP, QUERY_TYPE_TERM
            , QUERY_TYPE_TERMS, QUERY_TYPE_WILDCARD

    };

    /**
     * 三元关键词
     */
    private static final String[] THREE_UNARY_QUERY_TYPES = new String[] {
            QUERY_TYPE_RANGE
    };

    /**
     * 构建QueryBuilder
     * @param key
     * @param params
     * @return
     */
    static public QueryBuilder build(String key, Object params) {
        if (Arrays.asList(UNARY_QUERY_TYPES).contains(key)) {
            return buildUnaryQueryBuilder(key, params);
        } else if (Arrays.asList(TWO_UNARY_QUERY_TYPES).contains(key)) {
            return buildTwoUnaryQueryBuilder(key, (HashMap) params);
        } else if (Arrays.asList(THREE_UNARY_QUERY_TYPES).contains(key)) {
            return buildTreeUnaryQueryBuilder(key, (HashMap) params);
        }
        return null;
    }

    /**
     * 构建一元查询
     * @param key
     * @param params
     * @return
     */
    static public QueryBuilder buildUnaryQueryBuilder(String key, Object params) {
        try {
            Class<?> builder = Class.forName(QueryBuilders.class.toString());
            Method method = builder.getMethod(keyToMethodName(key), String.class);
            return (QueryBuilder) method.invoke(null, params);
        } catch (ClassNotFoundException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (NoSuchMethodException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (IllegalAccessException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (InvocationTargetException e) {
            log.error(DEFAULT_ERROR, e);
        }
        return null;
    }

    /**
     * 构建二元查询
     *
     * @param key
     * @param params
     * @return
     */
    static public QueryBuilder buildTwoUnaryQueryBuilder(String key, Map<String, Object> params) {
        assert params.containsKey(QUERY_PARAMS_FIELD);
        assert params.containsKey(QUERY_PARAMS_QUERY);
        if (QUERY_TYPE_COMMON_TERMS == key) {
            return QueryBuilders.commonTermsQuery((String) params.get(QUERY_PARAMS_FIELD),
                    QUERY_PARAMS_QUERY);
        }

        try {
            Class<?> builder = Class.forName(QueryBuilders.class.toString());
            Method method = builder.getMethod(keyToMethodName(key), String.class);
            return (QueryBuilder) method.invoke(null, params.get(QUERY_PARAMS_FIELD),
                    params.get(QUERY_PARAMS_QUERY));
        } catch (ClassNotFoundException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (NoSuchMethodException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (IllegalAccessException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (InvocationTargetException e) {
            log.error(DEFAULT_ERROR, e);
        }
        return null;
    }

    /**
     * 构建三元查询
     * @param key
     * @param params
     * @return
     */
    static public QueryBuilder buildTreeUnaryQueryBuilder(String key, Map<String, Object> params) {
        if (QUERY_TYPE_RANGE != key) {
            return null;
        }
        assert params.containsKey(QUERY_PARAMS_FIELD);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery((String ) params.get(
                QUERY_PARAMS_FIELD));
        if (params.containsKey(QUERY_PARAMS_LT)) {
            rangeQueryBuilder.lt(params.get(QUERY_PARAMS_LT));
        } else if (params.containsKey(QUERY_PARAMS_LTE)) {
            rangeQueryBuilder.lte(params.get(QUERY_PARAMS_LTE));
        } else if (params.containsKey(QUERY_PARAMS_TO)) {
            rangeQueryBuilder.to(params.get(QUERY_PARAMS_TO));
        }
        if (params.containsKey(QUERY_PARAMS_GT)) {
            rangeQueryBuilder.gt(params.get(QUERY_PARAMS_GT));
        } else if (params.containsKey(QUERY_PARAMS_GTE)) {
            rangeQueryBuilder.gte(params.get(QUERY_PARAMS_GTE));
        } else if (params.containsKey(QUERY_PARAMS_FROM)) {
            rangeQueryBuilder.from(params.get(QUERY_PARAMS_FROM));
        }
        return rangeQueryBuilder;
    }

    /**
     * 将下划线key转换为QueryBuilders的方法名
     * @param key
     * @return
     */
    static public String keyToMethodName(String key) {
        StringBuilder methodName = new StringBuilder();
        String[] keys = key.split("_");
        boolean first = true;
        for (String k : keys) {
            if (first) {
                methodName.append(ucFirst(k));
            } else {
                methodName.append(k);
            }
        }
        return methodName.toString();
    }

    /**
     * 首字母大写
     * @param word
     * @return
     */
    static public String ucFirst(String word) {
        char[] cs=word.toCharArray();
        cs[0]-=32;
        return String.valueOf(cs);
    }

}