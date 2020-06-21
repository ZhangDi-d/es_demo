package org.example.demo.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _1DocumentAPITest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_1DocumentAPITest.class);


    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * Index API
     *
     * @throws IOException
     */
    @Test
    public void test1Index() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        request.settings(Settings.builder().put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0));
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.toString());
    }

    @Test
    public void test2Index() throws IOException {
        //1
        IndexRequest request = new IndexRequest("posts");
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

        //2
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("posts").id("1").source(builder);
        //3
        IndexRequest source = new IndexRequest("posts").id("1").source("user", "kimchy");
        source.routing("routing");
        source.timeout(TimeValue.timeValueSeconds(2));
        source.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        source.version(2);
        source.versionType(VersionType.EXTERNAL);
        source.setPipeline("pipline");
        //4
        HashMap<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest2 = new IndexRequest("posts").id("1").source(jsonMap);

        IndexResponse response = restHighLevelClient.index(indexRequest2, RequestOptions.DEFAULT);

//异步执行
//        restHighLevelClient.indexAsync(source, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
//            @Override
//            public void onResponse(IndexResponse indexResponse) {
//            }
//
//            @Override
//            public void onFailure(Exception e) {
//                System.out.println("onFailure");
//            }
//        });

        String index = response.getIndex();
        System.out.println(index.toString());
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
    }

    /**
     * Get API
     *
     * @throws IOException
     */
    @Test
    public void test3GetIndex() throws IOException {
        GetRequest getRequest = new GetRequest("posts", "1");
        //getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);  //Configure source inclusion for specific fields

        String index = response.getIndex();
        String id = response.getId();
        if (response.isExists()) {
            String sourceAsString = response.getSourceAsString();
            long version = response.getVersion();
            byte[] sourceAsBytes = response.getSourceAsBytes();
            Map<String, Object> sourceAsMap = response.getSourceAsMap();
            System.out.println(sourceAsString);
        }
    }


    @Test
    public void test4IndexNotExist() throws IOException {
        GetRequest getRequest = new GetRequest("asasas", "1");
        try {
            GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("RestStatus.NOT_FOUND");
            }
            try {
                GetRequest request = new GetRequest("posts", "1").version(2);
                GetResponse getResponse = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.CONFLICT) {
                    System.out.println("RestStatus.CONFLICT");
                }
            }
        }
    }

    /**
     * Get Source API
     *
     * @throws IOException
     */
    @Test
    public void test5GetSourceAPI() {
        //GetSourceRequest getSourceRequest = new GetSourceRequest("posts", "1");
    }

    /**
     * Exists API
     */
    @Test
    public void test6GetSourceAPI() throws IOException {
        GetRequest getRequest = new GetRequest(
                "posts",
                "1");

        FetchSourceContext fetchSourceContext = new FetchSourceContext(false);
        getRequest.fetchSourceContext(fetchSourceContext); //Disable fetching _source
        getRequest.storedFields("_none_"); //Disable fetching stored fields
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * delete API
     */
    @Test
    public void test7DeleteAPI() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("posts", "1");
        deleteRequest.timeout(TimeValue.timeValueSeconds(2));
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        DeleteResponse response = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(response.toString());
        if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("DocWriteResponse.Result.NOT_FOUND");
        }
        //client.deleteAsync(request, RequestOptions.DEFAULT, listener);
    }

    /**
     * update API
     */
    @Test
    public void test8UpdateAPI() throws IOException {
        //1
        UpdateRequest updateRequest1 = new UpdateRequest("postqqqq", "1");
        Map<String, Object> parameters = singletonMap("count", 4);
        Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.field += params.count", parameters);
        updateRequest1.script(inline);

        //2
        String jsonString = "{" +
                "\"updated\":\"2017-01-01\"," +
                "\"reason\":\"daily update\"" +
                "}";
        UpdateRequest updateRequest2 = new UpdateRequest("post", "1");
        updateRequest2.doc(jsonString, XContentType.JSON);
        updateRequest2.docAsUpsert(true);
        updateRequest2.waitForActiveShards(ActiveShardCount.ONE);
        updateRequest2.timeout(TimeValue.timeValueSeconds(3));
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest2, RequestOptions.DEFAULT);
        System.out.println(updateResponse);
    }


    /**
     * Term Vectors API
     */
    @Test
    public void test9TermVectorsAPI() throws IOException {
        TermVectorsRequest request = new TermVectorsRequest("bank", "1");
        request.setFields("account_number", "balance");
        request.setFieldStatistics(false);
        request.setTermStatistics(true);
        request.setPositions(false);
        request.setOffsets(false);
        request.setPayloads(false);

        Map<String, Integer> filterSettings = new HashMap<>();
        filterSettings.put("max_num_terms", 3);
        filterSettings.put("min_term_freq", 1);
        filterSettings.put("max_term_freq", 10);
        filterSettings.put("min_doc_freq", 1);
        filterSettings.put("max_doc_freq", 100);
        filterSettings.put("min_word_length", 1);
        filterSettings.put("max_word_length", 10);

        request.setFilterSettings(filterSettings);

        Map<String, String> perFieldAnalyzer = new HashMap<>();
        perFieldAnalyzer.put("user", "keyword");
        request.setPerFieldAnalyzer(perFieldAnalyzer);
        request.setRealtime(false);

        TermVectorsResponse termvectors = restHighLevelClient.termvectors(request, RequestOptions.DEFAULT);
        System.out.println(termvectors.toString());

        termvectors.getTermVectorsList().forEach(tv -> {
            String fieldname = tv.getFieldName();
            int docCount = tv.getFieldStatistics().getDocCount();
            long sumTotalTermFreq = tv.getFieldStatistics().getSumTotalTermFreq();
            long sumDocFreq = tv.getFieldStatistics().getSumDocFreq();
            if (tv.getTerms() != null) {
                List<TermVectorsResponse.TermVector.Term> terms = tv.getTerms();
                for (TermVectorsResponse.TermVector.Term term : terms) {
                    String termStr = term.getTerm();
                    int termFreq = term.getTermFreq();
                    int docFreq = term.getDocFreq();
                    long totalTermFreq = term.getTotalTermFreq();
                    float score = term.getScore();
                    if (term.getTokens() != null) {
                        List<TermVectorsResponse.TermVector.Token> tokens = term.getTokens();
                        for (TermVectorsResponse.TermVector.Token token : tokens) {
                            int position = token.getPosition();
                            int startOffset = token.getStartOffset();
                            int endOffset = token.getEndOffset();
                            String payload = token.getPayload();
                        }
                    }
                }
            }
        });
    }

    /**
     * Bulk API
     */
    @Test
    public void test10BulkAPI() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("posts").id("1").source(XContentType.JSON, "name", "zhangsan"),
                new IndexRequest("posts").id("2").source(XContentType.JSON, "name", "lisi"),
                new IndexRequest("posts").id("3").source(XContentType.JSON, "name", "wangwu"));
        bulkRequest.timeout("2s");
        bulkRequest.setRefreshPolicy("wait_for");
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        BulkItemResponse[] items = bulkResponse.getItems();
        Arrays.stream(items).forEach(item -> {
            DocWriteRequest.OpType opType = item.getOpType();
            switch (opType) {
                case INDEX:
                    System.out.println("INDEX");
                case CREATE:
                    System.out.println("CREATE");
                case UPDATE:
                    System.out.println("UPDATE");
                case DELETE:
                    System.out.println("DELETE");
                default:

            }
        });
    }

    /**
     * Bulk Process API
     */
    @Test
    public void test11BulkProcessAPI() throws IOException {
        IndexRequest one = new IndexRequest("posts").id("1").source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts").id("2").source(XContentType.JSON, "title", "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts").id("3").source(XContentType.JSON, "title", "The Future of Federated Search in Elasticsearch");
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                int numberOfActions = bulkRequest.numberOfActions();
                logger.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    logger.warn("Bulk [{}] executed with failures", executionId);
                } else {
                    logger.debug("Bulk [{}] completed in {} milliseconds",
                            executionId, bulkResponse.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                logger.error("Failed to execute bulk", throwable);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(((bulkRequest, bulkResponseActionListener) -> {
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener);
        }), listener);

        builder.setBulkActions(5);
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(2);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        BulkProcessor bulkProcessor = null;
        try {
            //bulkProcessor
            bulkProcessor = builder.build();
            bulkProcessor.add(one);
            bulkProcessor.add(two);
            bulkProcessor.add(three);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            boolean b = false;
            try {
                assert bulkProcessor != null;
                b = bulkProcessor.awaitClose(2, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                bulkProcessor = null;
            }
        }
    }

    @Test
    public void test12MultiGetAPI() throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item("posts", "1").fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));
        String[] includes = new String[]{"a*", "balance"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.add(new MultiGetRequest.Item("bank", "2").fetchSourceContext(fetchSourceContext));

        MultiGetResponse mget = restHighLevelClient.mget(request, RequestOptions.DEFAULT);
        MultiGetItemResponse[] responses = mget.getResponses();
        assert responses != null;
        Arrays.stream(responses).forEach(response -> {
            GetResponse responseResponse = response.getResponse();
            assert responseResponse != null;
            System.out.println(responseResponse.getSourceAsMap());
        });


    }

    /**
     * Reindex API
     */
    @Test
    public void test13ReindexAPI() throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("bank");
        reindexRequest.setDestIndex("restored-bank"); //restored- 添加这个前缀，测试 pipeline API 是否生效：自动添加timestamp
        reindexRequest.setDestVersionType(VersionType.EXTERNAL); //The dest element can be configured like the index API to control optimistic concurrency control. Just leaving out versionType (as above) or setting it to internal will cause Elasticsearch to blindly dump documents into the target. Setting versionType to external will cause Elasticsearch to preserve the version from the source, create any documents that are missing, and update any documents that have an older version in the destination index than they do in the source index
        reindexRequest.setDestOpType("create");
        reindexRequest.setMaxDocs(100);
        reindexRequest.setSourceBatchSize(100);
        reindexRequest.setTimeout("2s");
        reindexRequest.setRefresh(true);
        //添加pipeline 可以处理字段，具体 https://www.elastic.co/guide/en/elasticsearch/reference/current/date-index-name-processor.html
        //并且注意 pipeline 中的processors ，不同的类型需要设置的字段也是不一样的 ，具体可以看这个 https://discuss.elastic.co/t/processor-remove-doesnt-support-one-or-more-provided-configuration-parameters-ignore-missing/147653
        reindexRequest.setDestPipeline("my-pipeline-03");
        BulkByScrollResponse bulkResponse = restHighLevelClient.reindex(reindexRequest, RequestOptions.DEFAULT);

        TimeValue timeTaken = bulkResponse.getTook();
        boolean timedOut = bulkResponse.isTimedOut();
        long totalDocs = bulkResponse.getTotal();
        long updatedDocs = bulkResponse.getUpdated();
        long createdDocs = bulkResponse.getCreated();
        long deletedDocs = bulkResponse.getDeleted();
        long batches = bulkResponse.getBatches();
        long noops = bulkResponse.getNoops();
        long versionConflicts = bulkResponse.getVersionConflicts();
        long bulkRetries = bulkResponse.getBulkRetries();
        long searchRetries = bulkResponse.getSearchRetries();
        TimeValue throttledMillis = bulkResponse.getStatus().getThrottled();
        TimeValue throttledUntilMillis = bulkResponse.getStatus().getThrottledUntil();
        List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures();
        List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
    }

    /**
     * Reindex API TASK
     */
    @Test
    public void test14ReindexAPI() throws IOException {
        ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices("bank");
        reindexRequest.setDestIndex("twitter");
        reindexRequest.setRefresh(true);

        TaskSubmissionResponse taskSubmissionResponse = restHighLevelClient.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);
        String taskId = taskSubmissionResponse.getTask();
        System.out.println(taskId);
    }

    /**
     * Update By Query API
     */
    @Test
    public void test15UpdateByQueryAPI() throws IOException {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest("twitter");
        updateByQueryRequest.setQuery(new TermQueryBuilder("account_number", "1"));
        //update
        updateByQueryRequest.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "if (ctx._source.firstname == 'Amber') {ctx._source.balance=400000;}",
                        Collections.emptyMap()));
        updateByQueryRequest.setConflicts("proceed");
        updateByQueryRequest.setMaxDocs(10);
        updateByQueryRequest.setTimeout(TimeValue.timeValueMinutes(2));
        updateByQueryRequest.setRefresh(true);
        BulkByScrollResponse bulkResponse = restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        logger.info("test15UpdateByQueryAPI, bulkResponse.getStatus={}", bulkResponse.getStatus());
    }

    /**
     * Delete By Query API
     */
    @Test
    public void test16DeleteByQueryAPI() throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest("twitter");
        deleteByQueryRequest.setQuery(new TermQueryBuilder("account_number", "1"));
        deleteByQueryRequest.setConflicts("proceed");
        deleteByQueryRequest.setMaxDocs(10);
        deleteByQueryRequest.setTimeout(TimeValue.timeValueMinutes(2));
        deleteByQueryRequest.setRefresh(true);
        deleteByQueryRequest.setConflicts("proceed");
        BulkByScrollResponse bulkByScrollResponse = restHighLevelClient.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        logger.info("test16DeleteByQueryAPI, bulkResponse.getStatus={}", bulkByScrollResponse.getStatus());
    }

    /**
     * Rethrottle API RethrottleRequest可用于更改正在运行的重新索引，按查询更新或按查询删除任务的当前限制，或完全禁用该任务的限制。
     */
    @Test
    public void test17Rethrottle() throws IOException {

    }

    /**
     * Multi Term Vectors API  Multi Term Vectors API allows to get multiple term vectors at once.
     *
     * @throws IOException
     */
    @Test
    public void test18MultiTermVector() throws IOException {
        MultiTermVectorsRequest multiTermVectorsRequest = new MultiTermVectorsRequest();
        TermVectorsRequest tvrequest1 = new TermVectorsRequest("twitter", "6");
        tvrequest1.setFields("account_number");
        multiTermVectorsRequest.add(tvrequest1);
        XContentBuilder docBuilder = XContentFactory.jsonBuilder();
        docBuilder.startObject().field("account_number", "13").endObject();
        TermVectorsRequest tvrequest2 = new TermVectorsRequest("twitter", docBuilder);
        multiTermVectorsRequest.add(tvrequest2);

        MultiTermVectorsResponse mtermvectors = restHighLevelClient.mtermvectors(multiTermVectorsRequest, RequestOptions.DEFAULT);
        List<TermVectorsResponse> tvresponseList = mtermvectors.getTermVectorsResponses();
        if (tvresponseList != null) {
            tvresponseList.forEach(termVectorsResponse -> {
                logger.info(termVectorsResponse.getId());
            });
        }
    }
}
