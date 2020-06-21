package org.example.demo.service;

import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.StoredScriptSource;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _10ScriptAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_10ScriptAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * Get Stored Script API
     */
    @Test
    public void test1GetStoredScriptAPI() throws IOException {
        GetStoredScriptRequest getStoredScriptRequest = new GetStoredScriptRequest("calculate-score");
        getStoredScriptRequest.masterNodeTimeout(TimeValue.timeValueSeconds(50));
        getStoredScriptRequest.masterNodeTimeout("50s");
        GetStoredScriptResponse scriptResponse = restHighLevelClient.getScript(getStoredScriptRequest, RequestOptions.DEFAULT);
        StoredScriptSource source = scriptResponse.getSource();
        logger.info("source={}", source);
    }

    /**
     * Put Stored Script API
     */
    @Test
    public void test2PutStoredScriptAPI() throws IOException {
        PutStoredScriptRequest putStoredScriptRequest = new PutStoredScriptRequest();
        putStoredScriptRequest.id("calculate-score");
        BytesArray bytesArray = new BytesArray("{\n" +
                "\"script\": {\n" +
                "\"lang\": \"painless\",\n" +
                "\"source\": \"Math.log(_score * 2) + params.multiplier\"" +
                "}\n" +
                "}\n");

        putStoredScriptRequest.content(bytesArray, XContentType.JSON);
        putStoredScriptRequest.timeout("2m");
        putStoredScriptRequest.masterNodeTimeout("1m");
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.putScript(putStoredScriptRequest, RequestOptions.DEFAULT);
        boolean acknowledged = acknowledgedResponse.isAcknowledged();
        logger.info("acknowledged={}", acknowledged);
    }

    /**
     * Delete Stored Script API
     */
    @Test
    public void test3DeleteStoredScriptAPI() throws IOException {
        DeleteStoredScriptRequest deleteStoredScriptRequest = new DeleteStoredScriptRequest("calculate-score");
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.deleteScript(deleteStoredScriptRequest, RequestOptions.DEFAULT);
        logger.info("acknowledged={}", acknowledgedResponse.isAcknowledged());
    }

}
