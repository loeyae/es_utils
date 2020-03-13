package com.loeyae.tools.es_utils.common;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ElasticSearchAggregationBuilderTest {

    private QueryBuilder queryBuilder;

    @Test
    void testBuild() {
        System.out.print(ElasticSearchAggregationBuilder.ALL_AGGREGATION_BUILDER_MAP);
        String jsonString = "{'avg': {'field': 'id'}}";
        List<AggregationBuilder> aggregationBuilderList = ElasticSearchAggregationBuilder.build(jsonString);
        assertNotNull(aggregationBuilderList);
        assertEquals(1, aggregationBuilderList.size());
        AggregationBuilder aggregationBuilder = aggregationBuilderList.get(0);
        assertTrue(aggregationBuilder instanceof AvgAggregationBuilder);
        assertEquals(AvgAggregationBuilder.NAME,
                aggregationBuilder.getName());
        assertEquals("id", ((AvgAggregationBuilder)aggregationBuilder).field());
        String jsonString1 = "[{'count': {'field': 'id'}}, {'sum': {'field': 'amount'}}]";
        List<AggregationBuilder> aggregationBuilderList1 =
                ElasticSearchAggregationBuilder.build(jsonString1);
        assertNotNull(aggregationBuilderList1);
        assertEquals(2, aggregationBuilderList1.size());
        String jsonString2 = "[{'count': {'field': 'id'}}, {'text': {'field': 'txt'}}]";
        List<AggregationBuilder> aggregationBuilderList2 =
                ElasticSearchAggregationBuilder.build(jsonString2);
        assertNotNull(aggregationBuilderList2);
        assertEquals(1, aggregationBuilderList2.size());
        AggregationBuilder aggregationBuilder1 = aggregationBuilderList2.get(0);
        assertTrue(aggregationBuilder1 instanceof ValueCountAggregationBuilder);
        String jsonString3 = "{'sum': 'amount'}";
        List<AggregationBuilder> aggregationBuilderList3 =
                ElasticSearchAggregationBuilder.build(jsonString3);
        assertNotNull(aggregationBuilderList3);
        assertEquals(1, aggregationBuilderList3.size());
        assertTrue(aggregationBuilderList3.get(0) instanceof SumAggregationBuilder);
        assertEquals("amount", ((SumAggregationBuilder)aggregationBuilderList3.get(0)).field());
        Map<String, Object> aggregation = new HashMap<String, Object>(){{
            put(AvgAggregationBuilder.NAME, "id");
        }};
        List<AggregationBuilder> aggregationBuilderList4 =
                ElasticSearchAggregationBuilder.build(aggregation);
        assertNotNull(aggregationBuilderList4);
        assertEquals(1, aggregationBuilderList4.size());
        assertTrue(aggregationBuilderList4.get(0) instanceof  AvgAggregationBuilder);
        assertEquals("id", ((AvgAggregationBuilder)aggregationBuilderList4.get(0)).field());
        Map<String, Object> aggregation1 = new HashMap<String, Object>(){{
            put(SumAggregationBuilder.NAME, "amount");
            put(ElasticSearchAggregationBuilder.AGGREGATION_TYPE_COUNT, "id");
        }};
        List<AggregationBuilder> aggregationBuilderList5 =
                ElasticSearchAggregationBuilder.build(aggregation1);
        assertNotNull(aggregationBuilderList5);
        assertEquals(2, aggregationBuilderList5.size());
        List<Map<String, Object>> aggregation2 = new ArrayList<Map<String, Object>>(){{
            add(new HashMap<String, Object>(){{
                put("avg", "id");
            }});
        }};
        List<AggregationBuilder> aggregationBuilderList6 =
                ElasticSearchAggregationBuilder.build(aggregation2);
        assertNotNull(aggregationBuilderList6);
        assertEquals(1, aggregationBuilderList6.size());
        List<Map<String, Object>> aggregation3 = new ArrayList<Map<String, Object>>(){{
            add(new HashMap<String, Object>(){{
                put("avg", "id");
            }});
            add(new HashMap<String, Object>(){{
                put("avg", new HashMap<String, Object>(){{
                    put(ElasticSearchAggregationBuilder.AGGREGATION_NAME_KEY, "amount_avg");
                    put(AggregationBuilder.CommonFields.FIELD.getPreferredName(), "amount");
                }});
            }});
        }};
        List<AggregationBuilder> aggregationBuilderList7 =
                ElasticSearchAggregationBuilder.build(aggregation3);
        assertNotNull(aggregationBuilderList7);
        assertEquals(2, aggregationBuilderList7.size());
        Set<String> amountAvg = new HashSet<>(1);
        aggregationBuilderList7.forEach(item -> {
            assertTrue(item instanceof AvgAggregationBuilder);
            if (item.getName() == "amount_avg") {
                amountAvg.add(item.getName());
            }
        });
        assertEquals(1, amountAvg.size());
    }

    @Test
    void testBuilder() {
        AggregationBuilder aggregationBuilder =
                ElasticSearchAggregationBuilder.builder(AvgAggregationBuilder.NAME, "id");
        assertNotNull(aggregationBuilder);
        assertTrue(aggregationBuilder instanceof AvgAggregationBuilder);
        assertEquals(AvgAggregationBuilder.NAME, aggregationBuilder.getName());
        assertEquals("id", ((AvgAggregationBuilder)aggregationBuilder).field());
        AggregationBuilder aggregationBuilder1 = ElasticSearchAggregationBuilder.builder("test",
                "id");
        assertNull(aggregationBuilder1);
        AggregationBuilder aggregationBuilder2 =
                ElasticSearchAggregationBuilder.builder(RangeQueryBuilder.NAME, new HashMap<String,
                        Object>(){{
                    put(AggregationBuilder.CommonFields.FIELD.getPreferredName(), "id");
                    put(RangeAggregator.RANGES_FIELD.getPreferredName(), new HashMap<String, Object>(){{
                        put(RangeAggregator.Range.FROM_FIELD.getPreferredName(), 1);
                        put(RangeAggregator.Range.TO_FIELD.getPreferredName(), 100);
                    }});
                }});
        assertNotNull(aggregationBuilder2);
        assertTrue(aggregationBuilder2 instanceof RangeAggregationBuilder);
        assertEquals("range", aggregationBuilder2.getName());
        assertEquals("id", ((RangeAggregationBuilder)aggregationBuilder2).field());
        RangeAggregator.Range range =
                ((RangeAggregationBuilder)aggregationBuilder2).ranges().iterator().next();
        assertEquals(1, range.getFrom());
        assertEquals(100, range.getTo());
        AggregationBuilder aggregationBuilder3 =
                ElasticSearchAggregationBuilder.builder(SumAggregationBuilder.NAME, new HashMap<String, Object>(){{
                    put(AggregationBuilder.CommonFields.FIELD.getPreferredName(), "id");
                    put(AggregationBuilder.CommonFields.VALUE_TYPE.getPreferredName(),
                            ValueType.DOUBLE.getPreferredName());
                }});
        assertNotNull(aggregationBuilder3);
        assertTrue(aggregationBuilder3 instanceof SumAggregationBuilder);
        assertEquals(ValueType.DOUBLE,
                ((SumAggregationBuilder)aggregationBuilder3).valueType());
        AggregationBuilder aggregationBuilder4 =
                ElasticSearchAggregationBuilder.builder(GlobalAggregationBuilder.NAME, null);
        assertNotNull(aggregationBuilder4);
        assertTrue(aggregationBuilder4 instanceof GlobalAggregationBuilder);
        AggregationBuilder aggregationBuilder5 =
                ElasticSearchAggregationBuilder.builder(NestedAggregationBuilder.NAME,
                        new HashMap<String, Object>(){{
                    put("path", "id");
                    put(ElasticSearchAggregationBuilder.SUB_AGGREGATION_KEY, new HashMap<String,
                     Object>(){{
                        put(FilterAggregationBuilder.NAME,
                                new HashMap<String, Object>(){{
                                    put(TermQueryBuilder.NAME, new HashMap<String, Object>(){{
                                        put("id", 1);
                                    }});
                                }});
                    }});
                }});
        assertNotNull(aggregationBuilder5);
        Collection<AggregationBuilder> aggregationBuilders =
                aggregationBuilder5.getSubAggregations();
        assertNotNull(aggregationBuilders);
        assertTrue(aggregationBuilders.size() == 1);
        AggregationBuilder aggregationBuilder6 = aggregationBuilders.iterator().next();
        assertTrue(aggregationBuilder6 instanceof FilterAggregationBuilder);
        assertNotNull(((FilterAggregationBuilder)aggregationBuilder6).getFilter());
        queryBuilder = ((FilterAggregationBuilder)aggregationBuilder6).getFilter();
        assertTrue(queryBuilder instanceof TermQueryBuilder);
        assertEquals("id", ((TermQueryBuilder) queryBuilder).fieldName());
        assertEquals(1, ((TermQueryBuilder) queryBuilder).value());
    }

}