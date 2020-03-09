package com.loeyae.tools.es_utils.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.adjacency.AdjacencyMatrixAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.sampler.SamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTextAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.scripted.ScriptedMetricAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.weighted_avg.WeightedAvgAggregationBuilder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public static final String AGGREGATION_TYPE_ADJACENCY_MATRIX = "adjacency_matrix";
    public static final String AGGREGATION_TYPE_AVG = "avg";
    public static final String AGGREGATION_TYPE_CARDINALITY = "cardinality";
    public static final String AGGREGATION_TYPE_COUNT = "count";
    public static final String AGGREGATION_TYPE_DATE_HISTOGRAM = "date_histogram";
    public static final String AGGREGATION_TYPE_DATE_RANGE = "date_range";
    public static final String AGGREGATION_TYPE_DIVERSIFIED_SAMPLER = "diversified_sampler";
    public static final String AGGREGATION_TYPE_EXTENDED_STATS = "extended_stats";
    public static final String AGGREGATION_TYPE_FILTER = "filter";
    public static final String AGGREGATION_TYPE_FILTERS = "filters";
    public static final String AGGREGATION_TYPE_GEO_BOUNDS = "geo_bounds";
    public static final String AGGREGATION_TYPE_GEO_CENTROID = "geo_centroid";
    public static final String AGGREGATION_TYPE_GEO_DISTANCE = "geo_distance";
    public static final String AGGREGATION_TYPE_GEOHASH_GRID = "geohash_grid";
    public static final String AGGREGATION_TYPE_GLOBAL = "global";
    public static final String AGGREGATION_TYPE_HISTOGRAM = "histogram";
    public static final String AGGREGATION_TYPE_IP_RANGE = "ip_range";
    public static final String AGGREGATION_TYPE_MAX = "max";
    public static final String AGGREGATION_TYPE_MIN = "min";
    public static final String AGGREGATION_TYPE_MISSING = "missing";
    public static final String AGGREGATION_TYPE_NESTED = "nested";
    public static final String AGGREGATION_TYPE_PERCENTILES = "percentiles";
    public static final String AGGREGATION_TYPE_PERCENTILE_RANKS = "percentile_ranks";
    public static final String AGGREGATION_TYPE_RANGE = "range";
    public static final String AGGREGATION_TYPE_REVERSE_NESTED = "reverse_nested";
    public static final String AGGREGATION_TYPE_SCRIPTED_METRIC = "scripted_metric";
    public static final String AGGREGATION_TYPE_SAMPLER = "sampler";
    public static final String AGGREGATION_TYPE_SIGNIFICANT_TERMS = "significant_terms";
    public static final String AGGREGATION_TYPE_SIGNIFICANT_TEXT = "significant_text";
    public static final String AGGREGATION_TYPE_STATS = "stats";
    public static final String AGGREGATION_TYPE_SUM = "sum";
    public static final String AGGREGATION_TYPE_WEIGHTED_AVG = "weighted_avg";
    public static final String AGGREGATION_TYPE_TERMS = "terms";
    public static final String AGGREGATION_TYPE_TOP_HITS = "top_hits";

    public static final Map<String, String> ALL_AGGREGATION_BUILDER_MAP = new HashMap<String, String>() {{
        put(AGGREGATION_TYPE_ADJACENCY_MATRIX, AdjacencyMatrixAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_AVG, AvgAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_CARDINALITY, CardinalityAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_COUNT, ValueCountAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_DATE_HISTOGRAM, DateHistogramAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_DATE_RANGE, DateRangeAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_DIVERSIFIED_SAMPLER, DiversifiedAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_EXTENDED_STATS, ExtendedStatsAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_FILTER, FilterAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_FILTERS, FiltersAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_GEO_BOUNDS, GeoBoundsAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_GEO_CENTROID, GeoCentroidAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_GEO_DISTANCE, GeoDistanceAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_GEOHASH_GRID, GeoGridAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_GLOBAL, GlobalAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_HISTOGRAM, HistogramAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_IP_RANGE, IpRangeAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_MAX, MaxAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_MIN, MinAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_MISSING, MissingAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_NESTED, NestedAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_PERCENTILE_RANKS, PercentileRanksAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_PERCENTILES, PercentilesAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_RANGE, RangeAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_REVERSE_NESTED, ReverseNestedAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_SAMPLER, SamplerAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_SCRIPTED_METRIC, ScriptedMetricAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_SIGNIFICANT_TERMS, SignificantTermsAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_SIGNIFICANT_TEXT, SignificantTextAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_STATS, StatsAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_SUM, SumAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_TERMS, TermsAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_TOP_HITS, TopHitsAggregationBuilder.class.getName());
        put(AGGREGATION_TYPE_WEIGHTED_AVG, WeightedAvgAggregationBuilder.class.getName());
    }};

    /**
     * build
     *
     * @param jsonString
     * @return
     */
    public List<AggregationBuilder> build(String jsonString) {
        Object jsonArray = JSONArray.parseArray(jsonString);
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
    public List<AggregationBuilder> build(List<Map<String, Object>> aggregations) {
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
    public List<AggregationBuilder> build(Map<String, Object> aggregations) {
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
    public AggregationBuilder builder(String key, Object aggregation) {
        String className = ALL_AGGREGATION_BUILDER_MAP.get(key);
        Map<String, Object> parsedAggregation = new HashMap<>();
        List<Map<String, Object>> subAggregation = new ArrayList<>();
        if (aggregation instanceof String) {
            parsedAggregation.put(AggregationBuilder.CommonFields.FIELD.getPreferredName(), aggregation);
        } else if (aggregation instanceof Map) {
            ((Map) aggregation).forEach((k, item) -> {
                if (SUB_AGGREGATION_KEY == k) {
                    subAggregation.add((Map<String, Object>) item);
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
                    xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonObject.toJSONString());
            AggregationBuilder aggregationBuilder = (AggregationBuilder) method.invoke(null, key,
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
    protected void buildSubAggregation(AggregationBuilder aggregationBuilder, List<Map<String,
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