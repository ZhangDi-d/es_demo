package org.example.demo.service;

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _7IngestAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_7IngestAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;
    /**
     * Ingest Node(预处理节点)是ES用于功能上命名的一种节点类型,可以通过在elasticsearch.xml进行如下配置来标识出集群中的某个节点是否是Ingest Node.
     *
     * node.ingest: false
     */

    /**
     * https://www.felayman.com/articles/2017/11/24/1511527532643.html
     */

    //ingest API 通俗的理解就是 加工数据

    /**
     * put Pipeline
     * get Pipeline
     * delete Pipline
     * Simulate Pipeline 官方也提供了相关的接口,来让我们对这些预处理操作进行测试,这些接口,官方称之为: Simulate Pipeline API.
     */
    /**
     * Put Pipeline API
     */
    @Test
    public void test1PutPipelineAPI() throws IOException {
        String source = "{\n" +
                "  \"description\": \"Adds a field to a document with the time of ingestion\",\n" +
                "  \"processors\": [\n" +
                "    {\n" +
                "      \"set\": {\n" +
                "        \"field\": \"ingest_timestamp\",\n" +
                "        \"value\": \"{{_ingest.timestamp}}\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        PutPipelineRequest request = new PutPipelineRequest(
                "my-pipeline-03",
                new BytesArray(source.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON
        );

        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.ingest().putPipeline(request, RequestOptions.DEFAULT);
        boolean acknowledged = acknowledgedResponse.isAcknowledged();
        logger.info("acknowledged={}", acknowledged);
    }


}
