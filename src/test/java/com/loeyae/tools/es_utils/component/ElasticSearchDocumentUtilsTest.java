package com.loeyae.tools.es_utils.component;

import org.elasticsearch.script.Script;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
class ElasticSearchDocumentUtilsTest {

    @Autowired
    ElasticSearchDocumentUtils utils;

    private static final String indexName = "zy-unit-test-sample";

    private static final String docType = "_doc";

    private static final String docId = "unit-test-1";

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testInsert() {
        Map<String, Object> params = new HashMap<>();
        params.put("id", "1");
        params.put("message", "message 1");
        params.put("created", System.currentTimeMillis());
        assertNotNull(utils);
        boolean ret = utils.insert(indexName, docType, params);
        assertTrue(ret);
        boolean ret1 = utils.insert(indexName, docType, docId, params);
        assertTrue(ret1);
    }

    @Test
    void testGet() {
        Map<String, Object> result = utils.get(indexName, docType, docId);
        assertNotNull(result);
        assertEquals("1", result.get("id"));
        assertEquals("message 1", result.get("message"));
    }

    @Test
    void testUpdate() {
        Map<String, Object> params = new HashMap<>();
        params.put("message", "message 2");
        boolean ret = utils.update(indexName, docType, docId, params);
        assertTrue(ret);
        Map<String, Object> params1 = new HashMap<>();
        params1.put("message", "message 10");
        Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, "ctx" +
                "._source.message=params.message", params1);
        boolean ret1 = utils.update(indexName, docType, docId, script);
        assertTrue(ret1);
        Script script1 = new Script("ctx._source.message='message 12'");
        boolean ret2 = utils.update(indexName, docType, docId, script1);
        assertTrue(ret2);
    }

    @Test
    void testDelete() {
        boolean ret = utils.delete(indexName, docType, docId);
        assertTrue(ret);
    }

    @Test
    void testBulkInsert() {
        List<Map<String, Object>> sources = new ArrayList<Map<String, Object>>(){
            {
                add(new HashMap<String, Object>(){
                    {
                        put("id", "10");
                        put("message", "bulk message 1");
                        put("created", new Date().getTime());
                    }
                });
                add(new HashMap<String, Object>(){
                    {
                        put("id", "11");
                        put("message", "bulk message 2");
                        put("created", new Date().getTime());
                    }
                });
            }
        };
        boolean[] retArray = utils.bulkInsert(indexName, docType, sources);
        assertTrue(retArray[0]);
        assertTrue(retArray[1]);
    }

    @Test
    void testUpdateByQuery() {
        Script script = new Script("ctx._source.id=ctx._source.id+1");
        long ret = utils.updateByQuery(indexName, docType, script, null);
        assertTrue(ret > 0);
        Script script1 = new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, "ctx" +
                "._source.id = Integer.parseInt(ctx._source.id) + params.count;", new HashMap<String,
                Object>(){{
                    put("count", 1);
        }});
        Map<String, Object> search = new HashMap<>();
        search.put("id", "101");
        long ret1 = utils.updateByQuery(indexName, docType, script1, search);
        assertEquals(1, ret1);
        Map<String, Object> settings = new HashMap<String, Object>(){{
            put("message", "message 112");
        }};
        Map<String, Object> search1 = new HashMap<String, Object>(){{
            put("id", "111");
        }};
        long ret2 = utils.updateByQuery(indexName, docType, settings, search1);
        assertEquals(1, ret2);
    }

    @Test
    void deleteByQuery() {
        Object exc = null;
        try {
            long ret1 = utils.deleteByQuery(indexName, docType, null);
        } catch (Exception e) {
            exc = e;
        } catch (Error e) {
            exc = e;
        }
        assertNotNull(exc);
        assertTrue(exc instanceof AssertionError);
        Map<String, Object> map1 = new HashMap<>();
        Object exc1 = null;
        try {
            long ret1 = utils.deleteByQuery(indexName, docType, map1);
        } catch (Exception e) {
            exc1 = e;
        } catch (Error e) {
            exc1 = e;
        }
        assertNotNull(exc1);
        assertTrue(exc1 instanceof AssertionError);

        Map<String, Object> map = new HashMap<String, Object>(){{
            put("id", new ArrayList<Integer>(){{
                add(0);
            }});
        }};
        long ret = utils.deleteByQuery(indexName, docType, map);
        assertTrue(ret > 0);
    }
}