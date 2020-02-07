package com.loeyae.tools.es_utils.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ElasticSearchQueryBuilderTest {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testBuild() {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        System.out.print(map.entrySet().toArray()[0]);
    }

    @Test
    void testBuildQueryBuilder() {
    }

    @Test
    void testMatchQueryBulder() {
    }

    @Test
    void testBuildMultiQueryBuilder() {
    }

    @Test
    void testContainsJoinKey() {
    }
}