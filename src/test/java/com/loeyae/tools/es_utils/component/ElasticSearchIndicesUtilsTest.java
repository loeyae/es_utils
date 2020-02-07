package com.loeyae.tools.es_utils.component;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
class ElasticSearchIndicesUtilsTest {

    private static final String indexName = "zy-unit-test-sample";

    @Autowired
    private ElasticSearchIndicesUtils utils;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testIndexExists() {
        assertNotNull(utils);
        boolean restStatus = utils.indexExists(indexName);
        assertFalse(restStatus);
    }

    @Test
    void testCreateIndexByName() {
        assertNotNull(utils);
        boolean restStatus = utils.createIndex(indexName);
        assertTrue(restStatus);
    }

    @Test
    void testCreateIndexBySettings() {
        if (utils.indexExists(indexName)) {
            utils.deleteIndex(indexName);
        }
        boolean restStatus = utils.createIndex(indexName, new Integer[]{10, 1});
        assertTrue(restStatus);
    }

    @Test
    void testCreateIndexByMapping() {
        if (utils.indexExists(indexName)) {
            utils.deleteIndex(indexName);
        }
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "long");
        fields.put("message", "text");
        fields.put("created", "date");
        boolean restStatus = utils.createIndex(indexName, "_doc", fields);
        assertTrue(restStatus);
    }

    @Test
    void testCreateIndexByFullParams() {

        if (utils.indexExists(indexName)) {
            utils.deleteIndex(indexName);
        }
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "long");
        fields.put("message", "text");
        fields.put("created", "date");
        boolean restStatus = utils.createIndex(indexName, new Integer[]{10, 1}, "_doc", fields);
        assertTrue(restStatus);
    }

    @Test
    void testDeleteIndex() {
        boolean restStatus = utils.deleteIndex(indexName);
        assertTrue(restStatus);
    }
}