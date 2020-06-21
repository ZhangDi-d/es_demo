package org.example.demo.service;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.SyncedFlushResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _5IndexAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_5IndexAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;


    /**
     * Analyze API
     */
    @Test
    public void test1AnalyzeAPI() throws IOException {
        AnalyzeRequest analyzeRequest1 = AnalyzeRequest.withGlobalAnalyzer("english", "Some text to analyze", "Some more text to analyze");

        Map<String, Object> stopFilter = new HashMap<>();
        stopFilter.put("type", "stop");
        stopFilter.put("stopwords", new String[]{"to", "a"});
        AnalyzeRequest analyzeRequest2 = AnalyzeRequest.buildCustomAnalyzer("standard")
                .addCharFilter("html_strip")
                .addTokenFilter("lowercase")
                .addTokenFilter(stopFilter)
                .build("Some text to analyze");

        AnalyzeRequest analyzeRequest3 = AnalyzeRequest.withGlobalAnalyzer("chinese", "中文测试奉命从");

        AnalyzeResponse response1 = restHighLevelClient.indices().analyze(analyzeRequest1, RequestOptions.DEFAULT);
        AnalyzeResponse response2 = restHighLevelClient.indices().analyze(analyzeRequest2, RequestOptions.DEFAULT);
        AnalyzeResponse response3 = restHighLevelClient.indices().analyze(analyzeRequest3, RequestOptions.DEFAULT);
        logger.info("response1={}", response1);
        logger.info("response2={}", response2);
    }

    /**
     * Create Index API
     */
    @Test
    public void test2CreateIndexAPI() throws IOException {
        //1
        CreateIndexRequest request = new CreateIndexRequest("twitter2");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0));

        request.mapping(
                "{\n" +
                        "  \"properties\": {\n" +
                        "    \"message\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("message");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder);

        request.source("{\n" +
                "    \"settings\" : {\n" +
                "        \"number_of_shards\" : 1,\n" +
                "        \"number_of_replicas\" : 0\n" +
                "    },\n" +
                "    \"mappings\" : {\n" +
                "        \"properties\" : {\n" +
                "            \"message\" : { \"type\" : \"text\" }\n" +
                "        }\n" +
                "    },\n" +
                "    \"aliases\" : {\n" +
                "        \"twitter_alias\" : {}\n" +
                "    }\n" +
                "}", XContentType.JSON);
    }

    /**
     * Create Index API
     */
    @Test
    public void test2CreateIndexAPI_2() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("twitter2");
        request.source("{\n" +
                "    \"settings\" : {\n" +
                "        \"number_of_shards\" : 1,\n" +
                "        \"number_of_replicas\" : 0\n" +
                "    },\n" +
                "    \"mappings\" : {\n" +
                "        \"properties\" : {\n" +
                "            \"message\" : { \"type\" : \"text\" }\n" +
                "        }\n" +
                "    },\n" +
                "    \"aliases\" : {\n" +
                "        \"twitter_alias\" : {}\n" +
                "    }\n" +
                "}", XContentType.JSON);

        request.setTimeout(TimeValue.timeValueMinutes(2));
        request.setMasterTimeout(TimeValue.timeValueMinutes(2));
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        logger.info("acknowledged={},shardsAcknowledged={}", acknowledged, shardsAcknowledged);
    }

    /**
     * Delete Index API
     */
    @Test
    public void test3DeleteIndexAPI() throws IOException {
        DeleteIndexRequest twitter2 = new DeleteIndexRequest("twitter2");
        twitter2.timeout(TimeValue.timeValueSeconds(2));
        twitter2.masterNodeTimeout("2s");
        twitter2.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(twitter2, RequestOptions.DEFAULT);

        boolean acknowledged = acknowledgedResponse.isAcknowledged();
        logger.info("acknowledged={}", acknowledged);

    }


    /**
     * Index Exists API
     */
    @Test
    public void test4IndexExistsAPI() throws IOException {
        GetIndexRequest request = new GetIndexRequest("twitter2");
        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);
        request.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        logger.info("acknowledged={}", exists);

    }


    /**
     * Open Index API  打开或关闭索引，使用close index api会使索引处于关闭状态，此时无法对该索引进行读、写，但索引数据不会被删除。
     */
    @Test
    public void test5OpenIndexAPI() throws IOException {
        OpenIndexRequest request = new OpenIndexRequest("twitter");
        request.timeout("2m");
        request.masterNodeTimeout("1m");
        request.waitForActiveShards(1);
        request.indicesOptions(IndicesOptions.strictExpandOpen());
        OpenIndexResponse openIndexResponse = restHighLevelClient.indices().open(request, RequestOptions.DEFAULT);
    }

    /**
     * close Index API
     *
     * @throws IOException
     */
    @Test
    public void test6CloseIndexAPI() throws IOException {
        CloseIndexRequest request = new CloseIndexRequest();
        request.setTimeout(TimeValue.timeValueMinutes(1));
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        AcknowledgedResponse closeIndexResponse = restHighLevelClient.indices().close(request, RequestOptions.DEFAULT);
    }

    /**
     * shrink Index API
     * 收缩索引。收缩索引API允许您将现有索引收缩为具有较少主碎片的新索引(下文为们称之为目标索引)。伸缩后的索引主分片的个数必须是原分片的公约数，举例说明，如果原先索引的个数为15，那伸缩后的索引主分片数量可以是3、5、1。
     */
    @Test
    public void test7ShrinkIndexAPI() throws IOException {
        org.elasticsearch.client.indices.ResizeRequest resizeRequest = new org.elasticsearch.client.indices.ResizeRequest("twitter2", "twitter");
        resizeRequest.setTimeout(TimeValue.timeValueSeconds(10));
        resizeRequest.setMasterTimeout(TimeValue.timeValueSeconds(30));
//        resizeRequest.getTargetIndexRequest().settings(Settings.builder()
//                .put("index.number_of_shards", 2)
//                .putNull("index.routing.allocation.require._name"));
        org.elasticsearch.client.indices.ResizeResponse resizeResponse = restHighLevelClient.indices().shrink(resizeRequest, RequestOptions.DEFAULT);

    }

    /**
     * Split Index API
     */
    @Test
    public void test7SplitIndexAPI() throws IOException {
        ResizeRequest resizeRequest = new ResizeRequest("twitter2", "twitter");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        resizeRequest.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards", 2));
        org.elasticsearch.action.admin.indices.shrink.ResizeResponse resizeResponse = restHighLevelClient.indices().split(resizeRequest, RequestOptions.DEFAULT);
    }

    /**
     * clone Index API
     */
    @Test
    public void test8CloneIndexAPI() throws IOException {
        ResizeRequest request = new ResizeRequest("target_index", "source_index");
        request.setResizeType(ResizeType.CLONE);
        request.masterNodeTimeout("1m");
        request.getTargetIndexRequest().alias(new Alias("target_alias"));
        org.elasticsearch.action.admin.indices.shrink.ResizeResponse resizeResponse = restHighLevelClient.indices().clone(request, RequestOptions.DEFAULT);
    }

    /**
     * Refresh API
     */
    @Test
    public void test9RefreshAPI() throws IOException {
        RefreshRequest refreshRequest = new RefreshRequest("twitter");
        refreshRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        RefreshResponse refresh = restHighLevelClient.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
        RestStatus status = refresh.getStatus();
        logger.info("test9RefreshAPI status={}", status);
    }

    /**
     * Flush API
     */
    @Test
    public void test10FlushAPI() throws IOException {
        FlushRequest flushRequest = new FlushRequest("twitter");
        flushRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        flushRequest.waitIfOngoing(true);
        flushRequest.force(true);
        FlushResponse flushResponse = restHighLevelClient.indices().flush(flushRequest, RequestOptions.DEFAULT);
        RestStatus flushResponseStatus = flushResponse.getStatus();
        logger.info("flushResponseStatus={}", flushResponseStatus);


    }

    /**
     * Flush synced API
     */
    @Test
    public void test11FlushSyncedAPI() throws IOException {
        SyncedFlushRequest request = new SyncedFlushRequest("twitter");
        request.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SyncedFlushResponse syncedFlushResponse = restHighLevelClient.indices().flushSynced(request, RequestOptions.DEFAULT);
    }

    /**
     * Clear Cache API
     * Force Merge API
     */
    /**
     * max_num_segments 合并为几个段，默认1
     * only_expunge_deletes 是否只合并含有删除文档的段，默认false
     * flush 合并后是否刷新，默认true
     */
    @Test
    public void test12ForceMergeAPI() throws IOException {
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest("twitter");
        forceMergeRequest.onlyExpungeDeletes(true); //only_expunge_deletes 是否只合并含有删除文档的段，默认false
        forceMergeRequest.maxNumSegments(1);
        forceMergeRequest.flush(true);
        ForceMergeResponse forceMergeResponse = restHighLevelClient.indices().forcemerge(forceMergeRequest, RequestOptions.DEFAULT);
        RestStatus status = forceMergeResponse.getStatus();
        logger.info("status={}", status);

    }


    /**
     * Rollover Index API
     */
    @Test
    public void test13RolloverIndexAPI() throws IOException {
        RolloverRequest request = new RolloverRequest("twitter_alias_0001", "twitter");
        request.addMaxIndexAgeCondition(new TimeValue(7, TimeUnit.DAYS));
        request.addMaxIndexDocsCondition(1000);
        request.addMaxIndexSizeCondition(new ByteSizeValue(5, ByteSizeUnit.GB));
        RolloverResponse rolloverResponse = restHighLevelClient.indices().rollover(request, RequestOptions.DEFAULT);

    }


    /**
     * Put Mapping API
     */
    @Test
    public void test14PutMappingAPI() throws IOException {
        PutMappingRequest putMappingRequest = new PutMappingRequest("twitter");
        putMappingRequest.source(
                "{\n" +
                        "  \"properties\": {\n" +
                        "    \"message\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        boolean acknowledged = acknowledgedResponse.isAcknowledged();

    }

    /**
     * Get Mappings API
     */

    /**
     * Get Field Mappings API
     */

    /**
     * Index Aliases API
     */
    @Test
    public void test15IndexAliasesAPI() throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(AliasActions.add().index("twitter").alias("twitter_alias_1"));

        AliasActions addIndexAction =
                new AliasActions(AliasActions.Type.ADD)
                        .index("index1")
                        .alias("alias1")
                        .filter("{\"term\":{\"year\":2016}}");
        AliasActions addIndicesAction =
                new AliasActions(AliasActions.Type.ADD)
                        .indices("index1", "index2")
                        .alias("alias2")
                        .routing("1");
        AliasActions removeAction =
                new AliasActions(AliasActions.Type.REMOVE)
                        .index("index3")
                        .alias("alias3");
        AliasActions removeIndexAction =
                new AliasActions(AliasActions.Type.REMOVE_INDEX)
                        .index("index4");

        indicesAliasesRequest.timeout(TimeValue.timeValueMinutes(2));
        indicesAliasesRequest.masterNodeTimeout("1m");

        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        logger.info("acknowledgedResponse={}", acknowledgedResponse.isAcknowledged());
    }

    /**
     * Delete Alias API
     */
    @Test
    public void test16DeleteAliasAPI() throws IOException {
        DeleteAliasRequest request = new DeleteAliasRequest("twitter", "twitter_alias_1");
        org.elasticsearch.client.core.AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().deleteAlias(request, RequestOptions.DEFAULT);
    }

    /**
     *  Exists Alias API
     */

    /**
     * Get Alias API
     */

    /**
     * Update Indices Settings API
     */

    /**
     * Get Settings API
     */

    /**
     * Put Template API
     */

    /**
     * Validate Query API
     */

    /**
     * Get Templates API
     */

    /**
     * Templates Exist API
     */

    /**
     * Get Index API
     */

    /**
     * Freeze Index API  为什么需要冻结index-》https://www.elastic.co/cn/blog/creating-frozen-indices-with-the-elasticsearch-freeze-index-api
     */

    /**
     *  Unfreeze Index API
     */

    /**
     * Delete Template API
     */

    /**
     * Reload Search Analyzers API
     */


}
