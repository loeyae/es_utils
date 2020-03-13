package com.loeyae.tools.es_utils.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.reflections.Reflections;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * ElasticSearch Aggregation Builder Factory.
 *
 * @date: 2020-03-06
 * @version: 1.0
 * @author: zhangyi07@beyondsoft.com
 */
@Slf4j
public class ElasticSearchAggregationBuilder {
    private static final String DEFAULT_ERROR = "ES Utils Error: ";
    private static final String PARSE_METHOD_NAME = "parse";
    public static final String SUB_AGGREGATION_KEY = "sub";
    public static final String AGGREGATION_NAME_KEY = "aggregation_key";
    public static final String AGGREGATION_TYPE_COUNT = "count";

    public static final Map<String, String> ALL_AGGREGATION_BUILDER_MAP = new HashMap<>();

    static {
        Reflections reflections = new Reflections("org.elasticsearch.search.aggregations.metrics");
        Set<Class<? extends AggregationBuilder>> aggregationBuilders =
                reflections.getSubTypesOf(AggregationBuilder.class);
        aggregationBuilders.forEach(item -> {
            if (!Modifier.isAbstract(item.getModifiers())) {
                try {
                    Field field = item.getField("NAME");
                    Object name = field.get(item);
                    ALL_AGGREGATION_BUILDER_MAP.put(name.toString(), item.getName());
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    log.error(DEFAULT_ERROR, e);
                }
            }
        });
        ALL_AGGREGATION_BUILDER_MAP.put(AGGREGATION_TYPE_COUNT,
                ValueCountAggregationBuilder.class.getName());
    }

    /**
     * build
     *
     * @param jsonString
     * @return
     */
    static public List<AggregationBuilder> build(String jsonString) {
        Object jsonArray = JSONArray.parse(jsonString);
        if (jsonArray instanceof Map) {
            return build((Map)jsonArray);
        } else if (jsonArray instanceof List) {
            return build((List)jsonArray);
        }
        return null;
    }

    /**
     * build
     *
     * @param aggregations
     * @return
     */
    static public List<AggregationBuilder> build(List<Map<String, Object>> aggregations) {
        List<AggregationBuilder> aggregationBuilderList = new ArrayList<>();
        aggregations.forEach((item -> {
            List<AggregationBuilder> aggregationBuilders = build(item);
            if (null != aggregationBuilders && aggregationBuilders.size() > 0) {
                aggregationBuilderList.addAll(aggregationBuilders);
            }
        }));
        return aggregationBuilderList;
    }

    /**
     * build
     *
     * @param aggregations
     * @return
     */
    static public List<AggregationBuilder> build(Map<String, Object> aggregations) {
        List<AggregationBuilder> aggregationBuilderList = new ArrayList<>();
        aggregations.forEach((String key, Object item) -> {
            if (ALL_AGGREGATION_BUILDER_MAP.containsKey(key)) {
                AggregationBuilder aggregationBuilder = builder(key, item);
                if (null != aggregationBuilder) {
                    aggregationBuilderList.add(aggregationBuilder);
                }
            }
        });
        return aggregationBuilderList;
    }

    /**
     * builder
     *
     * @param key
     * @param aggregation
     * @return
     */
    static public AggregationBuilder builder(String key, Object aggregation) {
        String className = ALL_AGGREGATION_BUILDER_MAP.get(key);
        if (null == className) {
            return null;
        }
        Map<String, Object> parsedAggregation = new HashMap<>();
        List<Map<String, Object>> subAggregation = new ArrayList<>();
        Set<String> aggregationNameSet = new HashSet<>();
        if (aggregation instanceof String) {
            parsedAggregation.put(AggregationBuilder.CommonFields.FIELD.getPreferredName(), aggregation);
        } else if (aggregation instanceof Map) {
            ((Map) aggregation).forEach((k, item) -> {
                if (SUB_AGGREGATION_KEY == k) {
                    subAggregation.add((Map<String, Object>) item);
                } else if (AGGREGATION_NAME_KEY == k) {
                    aggregationNameSet.add(item.toString());
                } else {
                    parsedAggregation.put(String.valueOf(k), item);
                }
            });
        }
        try {
            Class<?> aggregationBuilderClass = Class.forName(className);
            Method method = aggregationBuilderClass.getMethod(PARSE_METHOD_NAME, String.class,
                    XContentParser.class);
            JSONObject jsonObject = new JSONObject(parsedAggregation);
            XContentType xContentType = XContentType.JSON;
            XContentParser xContentParser =
                    xContentType.xContent().createParser(ElasticSearchQueryBuilder.namedXContentRegistry,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonObject.toJSONString());
            if (null == xContentParser.currentToken()) {
                xContentParser.nextToken();
            }
            String aggregationName = key;
            if (aggregationNameSet.size() > 0) {
                aggregationName = aggregationNameSet.iterator().next();
            }
            AggregationBuilder aggregationBuilder = (AggregationBuilder) method.invoke(null, aggregationName,
                    xContentParser);
            buildSubAggregation(aggregationBuilder, subAggregation);
            return aggregationBuilder;
        }catch (ClassNotFoundException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (NoSuchMethodException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (IOException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (IllegalAccessException e) {
            log.error(DEFAULT_ERROR, e);
        } catch (InvocationTargetException e) {
            log.error(DEFAULT_ERROR, e);
        }
        return null;
    }

    /**
     * buildSubAggregation
     *
     * @param aggregationBuilder
     * @param subAggregation
     */
    static protected void buildSubAggregation(AggregationBuilder aggregationBuilder, List<Map<String,
            Object>> subAggregation) {
        if (subAggregation.size() > 0) {
            subAggregation.forEach((item) -> {
                List<AggregationBuilder> aggregationBuilders = build(item);
                aggregationBuilders.forEach((i) -> {
                    aggregationBuilder.subAggregation(i);
                });
            });
        }
    }

}