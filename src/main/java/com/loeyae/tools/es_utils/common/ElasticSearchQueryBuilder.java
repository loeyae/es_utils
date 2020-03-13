package com.loeyae.tools.es_utils.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.*;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

/**
 * elastic search search tool.
 *
 * @date: 2020-02-06
 * @version: 1.0
 * @author: zhangyi07@beyondsoft.com
 */
@Slf4j
public class ElasticSearchQueryBuilder {

    private static final String DEFAULT_ERROR = "ES Utils Error: ";
    public static final String JOIN_TYPE_MUST = "must";
    public static final String JOIN_TYPE_MUST_NOT = "must_not";
    public static final String JOIN_TYPE_SHOULD = "should";
    public static final String JOIN_TYPE_FILTER = "filter";
    public static final Set ALL_QUERY_TYPES = new HashSet<String>();

    public static final NamedXContentRegistry namedXContentRegistry;
    public static final Set<String> joinKeySet = new HashSet<>(4);

    static {

        joinKeySet.add(JOIN_TYPE_MUST);
        joinKeySet.add(JOIN_TYPE_MUST_NOT);
        joinKeySet.add(JOIN_TYPE_SHOULD);
        joinKeySet.add(JOIN_TYPE_FILTER);

        Reflections reflections = new Reflections("org.elasticsearch.index.query");
        Set<Class<? extends AbstractQueryBuilder>> queryBuilderClasses =
                reflections.getSubTypesOf(AbstractQueryBuilder.class);
        Map<String, ContextParser<Object, ? extends QueryBuilder>> parserMap = new HashMap<String,
                ContextParser<Object, ? extends QueryBuilder>>(queryBuilderClasses.size());
        queryBuilderClasses.forEach(item -> {
            if (!Modifier.isAbstract(item.getModifiers())) {
                try {
                    Field field = item.getField("NAME");
                    Object name = field.get(item);
                    Method parser = item.getMethod("fromXContent", XContentParser.class);
                    parserMap.put(name.toString(), (p, c) -> {
                        try {
                            return (QueryBuilder) parser.invoke(null, p);
                        } catch (IllegalAccessException | InvocationTargetException e) {
                            log.error(DEFAULT_ERROR, e);
                        }
                        return null;
                    });
                    ALL_QUERY_TYPES.add(name);
                } catch (NoSuchFieldException | NoSuchMethodException | IllegalAccessException e) {
                    log.error(DEFAULT_ERROR, e);
                }
            }
        });
        List<NamedXContentRegistry.Entry> entries = parserMap.entrySet().stream()
                .map(entry -> new NamedXContentRegistry.Entry(QueryBuilder.class,
                        new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        namedXContentRegistry = new NamedXContentRegistry(entries);

    }

    /**
     * 构建查询
     *
     * @param query
     * @return
     */
    static public QueryBuilder build(Map<String, Object> query) {
        if (null == query) {
            return build();
        }
        if (query.size() == 1 && containsJoinKey(query) == false) {
            return queryBuilder(query);
        }
        return boolQueryBuilder(query);
    }

    /**
     * 构建查询
     *
     * @param params
     * @return
     */
    static public QueryBuilder build(List<Map<String, Object>> params) {
        if (null == params) {
            return build();
        }
        Map<String, Object> query = new HashMap<>();
        query.put(ElasticSearchQueryBuilder.JOIN_TYPE_MUST, params);
        return build(query);
    }

    /**
     * 构建查询
     *
     * @param jsonString
     * @return
     */
    static public QueryBuilder build(String jsonString) {
        if (null == jsonString) {
            return build();
        }
        JSON json = JSON.parseObject(jsonString);
        Object query = JSONPath.eval(json, "$");
        if (query instanceof Map) {
            return build((Map) query);
        }
        return build((List) query);
    }

    /**
     * 构建查询
     *
     * @return
     */
    static public QueryBuilder build() {
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        return matchAllQueryBuilder;
    }

    /**
     * boolQueryBuilder
     *
     * @param query
     * @return
     */
    static public BoolQueryBuilder boolQueryBuilder(Map<String, Object> query) {
        Map<String, Object> filteredQuery = null;
        if (containsJoinKey(query)) {
            filteredQuery = filterBoolQuery(query);
        } else {
            filteredQuery = computedBoolQuery(query);
        }
        Map<String, Object> finalFilteredQuery = filteredQuery;
        Map<String, Object> boolQuery = new HashMap<String, Object>(1) {{
            put(BoolQueryBuilder.NAME, finalFilteredQuery);
        }};
        return (BoolQueryBuilder) queryBuilder(boolQuery);
    }

    /**
     * queryBuilder
     *
     * @param query
     * @return
     */
    static public QueryBuilder queryBuilder(Map<String, Object> query) {
        Map.Entry current = query.entrySet().iterator().next();
        Map<String, Object> computed = computedQuery(current.getKey().toString(), current.getValue());
        JSONObject jsonObject = new JSONObject(computed);
        XContentType xContentType = XContentType.JSON;
        try {
            XContentParser xContentParser =
                    xContentType.xContent().createParser(namedXContentRegistry,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonObject.toJSONString());
            return AbstractQueryBuilder.parseInnerQueryBuilder(xContentParser);
        } catch (IOException e) {
            log.error(DEFAULT_ERROR, e);
        }
        return null;
    }

    /**
     * computedBoolQuery
     *
     * @param query
     * @return
     */
    static protected Map<String, Object> computedBoolQuery(Map<String, Object> query) {
        List<Map<String, Object>> computedBoolQuery = new ArrayList<>();
        query.forEach((key, item) -> {
            computedBoolQuery.add(computedQuery(key, item));
        });
        return new HashMap<String, Object>() {{
            put(JOIN_TYPE_MUST, computedBoolQuery);
        }};
    }

    /**
     * computedQuery
     *
     * @param key
     * @param query
     * @return
     */
    static protected Map<String, Object> computedQuery(String key, Object query) {
            if (ALL_QUERY_TYPES.contains(key)) {
                return new HashMap<String, Object>(1) {{
                    put(key, query);
                }};
            }
            if (query instanceof Object[]) {
                return new HashMap<String, Object>(){{
                    put(TermsQueryBuilder.NAME, new HashMap<String, Object>(1) {{
                        put(key, query);
                    }});
                }};
            }
            if (query instanceof List) {
                Map<String, Object> range = new HashMap<>();
                Iterator iterator = ((List) query).iterator();
                range.put(RangeQueryBuilder.FROM_FIELD.getPreferredName(), iterator.next());
                if (iterator.hasNext()) {
                    range.put(RangeQueryBuilder.TO_FIELD.getPreferredName(), iterator.next());
                }
                return new HashMap<String, Object>(1){{
                    put(RangeQueryBuilder.NAME, new HashMap<String, Object>(1) {{
                        put(key, range);
                    }});
                }};
            }
            if (query instanceof Map) {
                return new HashMap<String, Object>(1){{
                    put(RangeQueryBuilder.NAME, new HashMap<Object, Object>(1) {{
                        put(key, query);
                    }});
                }};
            }

            return new HashMap<String, Object>(1){{
                put(TermQueryBuilder.NAME, new HashMap<String, Object>(1) {{
                    put(key, query.toString());
                }});
            }};
    }

    /**
     * filterBoolQuery
     *
     * @param query
     * @return
     */
    static protected Map<String, Object> filterBoolQuery(Map<String, Object> query) {
        Map<String, Object> filteredQuery = new HashMap<>(4);
        query.forEach((key, item) -> {
            if (joinKeySet.contains(key)) {
                filteredQuery.put(key, listQuery(item));
            }
        });
        return filteredQuery;
    }

    /**
     * listQuery
     *
     * @param query
     * @return
     */
    static protected List<Map<String, Object>> listQuery(Object query) {
        if (query instanceof List) {
            List<Map<String, Object>> mapList = new ArrayList<>();
            ((List) query).forEach(item -> {
                mapList.addAll(listQuery(item));
            });
            return mapList;
        }
        if (query instanceof Map) {
            List<Map<String, Object>> mapList = new ArrayList<>();
            ((Map) query).forEach((key, item) -> {
                mapList.add(computedQuery(key.toString(), item));
            });
            return mapList;
        }
        return null;
    }

    /**
     * 判断是否存在连接关键词
     *
     * @param query
     * @return
     */
    static public boolean containsJoinKey(Map<String, Object> query) {
        Set<String> keySet = new HashSet<String>(){{
            addAll(joinKeySet);
        }};
        keySet.retainAll(query.keySet());
        return keySet.size() > 0;
    }

}