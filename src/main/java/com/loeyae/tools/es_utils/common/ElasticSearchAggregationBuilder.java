package com.loeyae.tools.es_utils.common;

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

import static com.alibaba.fastjson.JSON.parse;
import static org.elasticsearch.common.ParseField.CommonFields.FIELD;

/**
 * ElasticSearch Aggregation Builder Factory.
 *
 * @date 2020-03-06
 * @version 1.0
 * @author zhangyi<loeyae@gmail.com>
 */
@Slf4j
public class ElasticSearchAggregationBuilder {

    private static final String DEFAULT_ERROR = "ES Utils Error: ";
    private static final String PARSE_METHOD_NAME = "parse";
    public static final String SUB_AGGREGATION_KEY = "sub";
    public static final String AGGREGATION_NAME_KEY = "aggregation_key";
    public static final String AGGREGATION_TYPE_COUNT = "count";

    protected static final Map<String, String> ALL_AGGREGATION_BUILDER_MAP = new HashMap<>();

    static {
        Reflections reflections = new Reflections("org.elasticsearch.search.aggregations");
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

    private ElasticSearchAggregationBuilder() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * build
     *
     * @param aggregation query
     * @return List of AggregationBuilder
     */
    @SuppressWarnings("unchecked")
    public static List<AggregationBuilder> build(Object aggregation) {
        if (aggregation instanceof Map) {
            return build((Map<String, Object>)aggregation);
        } else if (aggregation instanceof List) {
            return build((List<Map<String, Object>>) aggregation);
        } else {
            return build(aggregation.toString());
        }
    }

    /**
     * build
     *
     * @param jsonString query string
     * @return List of AggregationBuilder
     */
    @SuppressWarnings("unchecked")
    public static List<AggregationBuilder> build(String jsonString) {
        Object jsonArray = parse(jsonString);
        List<AggregationBuilder> aggregationBuilderList = new ArrayList<>();
        if (jsonArray instanceof Map) {
            aggregationBuilderList = build((Map<String, Object>) jsonArray);
        } else if (jsonArray instanceof List) {
            aggregationBuilderList = build((List<Map<String, Object>>) jsonArray);
        }
        return aggregationBuilderList;
    }

    /**
     * build
     *
     * @param aggregations Map of aggregation
     * @return List of AggregationBuilder
     */
    public static List<AggregationBuilder> build(List<Map<String, Object>> aggregations) {
        List<AggregationBuilder> aggregationBuilderList = new ArrayList<>();
        aggregations.forEach((item -> {
            List<AggregationBuilder> aggregationBuilders = build(item);
            if (!aggregationBuilders.isEmpty()) {
                aggregationBuilderList.addAll(aggregationBuilders);
            }
        }));
        return aggregationBuilderList;
    }

    /**
     * build
     *
     * @param aggregations Map of aggregation
     * @return List of AggregationBuilder
     */
    public static List<AggregationBuilder> build(Map<String, Object> aggregations) {
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
     * @param key         Name of AggregationBuilder
     * @param aggregation Map of aggregation
     * @return instance of AggregationBuilder
     */
    @SuppressWarnings("unchecked")
    public static AggregationBuilder builder(String key, Object aggregation) {
        String className = ALL_AGGREGATION_BUILDER_MAP.get(key);
        if (null == className) {
            return null;
        }
        Map<String, Object> parsedAggregation = new HashMap<>();
        List<Map<String, Object>> subAggregation = new ArrayList<>();
        Set<String> aggregationNameSet = new HashSet<>();
        if (aggregation instanceof String) {
            parsedAggregation.put(FIELD.getPreferredName(), aggregation);
        } else if (aggregation instanceof Map) {
            ((Map<String, Object>) aggregation).forEach((k, item) -> {
                if (SUB_AGGREGATION_KEY.equals(k)) {
                    subAggregation.add((Map<String, Object>) item);
                } else if (AGGREGATION_NAME_KEY.equals(k)) {
                    aggregationNameSet.add(item.toString());
                } else {
                    parsedAggregation.put(String.valueOf(k), item);
                }
            });
        } else {
            throw new IllegalArgumentException();
        }
        try {
            Class<?> aggregationBuilderClass = Class.forName(className);
            Method method = aggregationBuilderClass.getMethod(PARSE_METHOD_NAME, String.class,
                    XContentParser.class);
            JSONObject jsonObject = new JSONObject(parsedAggregation);
            XContentType xContentType = XContentType.JSON;
            AggregationBuilder aggregationBuilder;
            try (XContentParser xContentParser = xContentType.xContent().createParser(ElasticSearchQueryBuilder.namedXContentRegistry,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonObject.toJSONString())) {
                if (null == xContentParser.currentToken()) {
                    xContentParser.nextToken();
                }
                String aggregationName = key;
                if (!aggregationNameSet.isEmpty()) {
                    aggregationName = aggregationNameSet.iterator().next();
                }
                aggregationBuilder = (AggregationBuilder) method.invoke(null, aggregationName,
                        xContentParser);
            }
            buildSubAggregation(aggregationBuilder, subAggregation);
            return aggregationBuilder;
        } catch (ClassNotFoundException | NoSuchMethodException | IOException | IllegalAccessException | InvocationTargetException e) {
            log.error(DEFAULT_ERROR, e);
        }
        return null;
    }

    /**
     * buildSubAggregation
     *
     * @param aggregationBuilder instance of AggregationBuilder
     * @param subAggregation List of aggregation
     */
    protected static void buildSubAggregation(AggregationBuilder aggregationBuilder, List<Map<String,
            Object>> subAggregation) {
        if (!subAggregation.isEmpty()) {
            subAggregation.forEach((item) -> {
                List<AggregationBuilder> aggregationBuilders = build(item);
                aggregationBuilders.forEach(aggregationBuilder::subAggregation);
            });
        }
    }

}