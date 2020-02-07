package com.loeyae.tools.es_utils.common;

import javafx.beans.binding.ObjectExpression;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queryparser.xml.QueryBuilderFactory;
import org.elasticsearch.index.query.*;

import java.util.*;

/**
 * elastic search search tool.
 *
 * @date: 2020-02-06
 * @version: 1.0
 * @author: zhangyi07@beyondsoft.com
 */
public class ElasticSearchQueryBuilder {

    public static final String JOIN_TYPE_MUST = "must";
    public static final String JOIN_TYPE_MUST_NOT = "must_not";
    public static final String JOIN_TYPE_SHOULD = "should";

    /**
     * 构建查询
     *
     * @param query
     * @return
     */
    static public QueryBuilder build(Map<String, Object>query) {
        QueryBuilders.idsQuery();
        if (null == query) {
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            return matchAllQueryBuilder;
        }
        if (query.size() == 1) {
            for (Map.Entry<String, Object> item : query.entrySet()) {
                return ElasticSearchQueryFactory.build(item.getKey(), item.getValue());
            }
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        buildMultiQueryBuilder(boolQueryBuilder, query);
        return boolQueryBuilder;
    }

    /**
     * 构建复合查询
     * @param queryBuilder
     * @param query
     */
    static public void buildMultiQueryBuilder(BoolQueryBuilder queryBuilder,
                                              Map<String, Object>query) {
        if (containsJoinKey(query)) {
            Object mustQuery = query.get(JOIN_TYPE_MUST);
            Object mustNotQuery= query.get(JOIN_TYPE_MUST_NOT);
            Object shouldQuery = query.get(JOIN_TYPE_SHOULD);
            if (null != mustQuery && mustQuery instanceof List) {
                for (Map<String, Object> mq: (List<Map>) mustQuery){
                    buildMustQuery(queryBuilder, mq);
                }
            }
            if (null != mustNotQuery && mustNotQuery instanceof List) {
                for (Map<String, Object> mnq: (List<Map>) mustNotQuery){
                    buildMustNotQuery(queryBuilder, mnq);
                }
            }
            if (null != shouldQuery && shouldQuery instanceof List) {
                for (Map<String, Object> sq: (List<Map>) shouldQuery){
                    buildShouldQuery(queryBuilder, sq);
                }
            }
        } else {
            buildMustQuery(queryBuilder, query);
        }
    }

    /**
     * 构建must查询
     * @param queryBuilder
     * @param query
     */
    static public void buildMustQuery(BoolQueryBuilder queryBuilder, Map<String, Object>query) {
        query.forEach((key, value)->{
            QueryBuilder qb = ElasticSearchQueryFactory.build(key, value);
            if (null != qb) {
                queryBuilder.must(qb);
            }
        });
    }

    /**
     * 构建must not查询
     * @param queryBuilder
     * @param query
     */
    static public void buildMustNotQuery(BoolQueryBuilder queryBuilder, Map<String, Object>query) {
        query.forEach((key, value)->{
            QueryBuilder qb = ElasticSearchQueryFactory.build(key, value);
            if (null != qb) {
                queryBuilder.mustNot(qb);
            }
        });
    }

    /**
     * 构建should查询
     *
     * @param queryBuilder
     * @param query
     */
    static public void buildShouldQuery(BoolQueryBuilder queryBuilder, Map<String, Object>query) {
        query.forEach((key, value)->{
            QueryBuilder qb = ElasticSearchQueryFactory.build(key, value);
            if (null != qb) {
                queryBuilder.should(qb);
            }
        });
    }

    /**
     * 判断是否存在连接关键词
     *
     * @param query
     * @return
     */
    static public boolean containsJoinKey(Map<String, Object>query) {
        Set<String> joinKeySet = new HashSet<>();
        joinKeySet.add(JOIN_TYPE_MUST);
        joinKeySet.add(JOIN_TYPE_MUST_NOT);
        joinKeySet.add(JOIN_TYPE_SHOULD);
        joinKeySet.retainAll(query.keySet());
        return joinKeySet.size() > 0;
    }

}