package com.loeyae.tools.es_utils.component;

import com.loeyae.tools.es_utils.common.ElasticSearchQueryBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
class ElasticSearchQueryUtilsTest {

    @Autowired
    ElasticSearchQueryUtils utils;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testSearchByNull() {
        SearchResponse searchResponse = utils.search((String )null, 100,
                ElasticSearchQueryUtils.QUERY_FORM_NULL, null, null, null);
        assertNotNull(searchResponse);
        assertTrue(RestStatus.OK == searchResponse.status());
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertNull(result.getScrollId());
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getSource().size() > 0);
        SearchResponse searchResponse1 = utils.search((String)null, 100, 0, new HashMap<String, Integer>(){{
            put("id", 1);
        }}, null, null);
        assertNotNull(searchResponse1);
        assertTrue(RestStatus.OK == searchResponse1.status());
        ElasticSearchQueryUtils.Result result1 = ElasticSearchQueryUtils.result(searchResponse1);
        assertNull(result1.getScrollId());
        assertTrue(result1.getTotal() > 0);
        assertTrue(result1.getSource().size() > 0);
        assertTrue(result.toString().equals(result1.toString()));
        SearchResponse searchResponse2 = utils.search((String)null, 100, 0, new HashMap<String,
                Integer>(){{
            put("id", -1);
        }}, null, null);
        assertNotNull(searchResponse2);
        assertTrue(RestStatus.OK == searchResponse2.status());
        ElasticSearchQueryUtils.Result result2 = ElasticSearchQueryUtils.result(searchResponse2);
        assertNull(result2.getScrollId());
        assertTrue(result2.getTotal() > 0);
        assertTrue(result2.getSource().size() > 0);
        assertFalse(result.toString().equals(result2.toString()));
    }

    @Test
    void testSearchByMap() {
        Map<String, Object> search = new HashMap<String, Object>(){{
            put("id", "1");
        }};
        SearchResponse searchResponse = utils.search(search, 1,
                ElasticSearchQueryUtils.QUERY_FORM_NULL, null, null, null);
        assertNotNull(searchResponse);
        assertTrue(RestStatus.OK == searchResponse.status());
        ElasticSearchQueryUtils.Result result = ElasticSearchQueryUtils.result(searchResponse);
        assertNull(result.getScrollId());
        assertTrue(result.getTotal() > 0);
        assertTrue(result.getSource().size() > 0);
    }

    @Test
    void testSearchByList() {
        List<Map<String, Object>> search = new ArrayList<Map<String, Object>>(){{
            add(new HashMap<String, Object>(){{
                put("id", 1);
            }});
        }};
    }
}