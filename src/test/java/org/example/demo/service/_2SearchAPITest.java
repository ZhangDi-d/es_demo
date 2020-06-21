package org.example.demo.service;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.rankeval.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _2SearchAPITest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_2SearchAPITest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;


    /**
     * The SearchRequest is used for any operation that has to do with searching documents, aggregations, suggestions and also offers ways of requesting highlighting on the resulting documents.
     * <p>
     * searchRequest ->searchSourceBuilder ->QueryBuilder
     */
    @Test
    public void test1SearchRequest() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); //MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        //不同的QueryBuilder
        //MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("account_number",13);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(2));
        //sort
        searchSourceBuilder.sort("balance", SortOrder.DESC);
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));

        //fetchSource
        searchSourceBuilder.fetchSource(false);
        String[] includes = new String[]{"account_number", "a*", "s*", "firstname"};
        String[] excludes = new String[]{};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        searchSourceBuilder.fetchSource(fetchSourceContext);

        //Highlighting search
        HighlightBuilder highlightBuilder = new HighlightBuilder();

        HighlightBuilder.Field firstname = new HighlightBuilder.Field("firstname");
        highlightBuilder.field(firstname);
        searchSourceBuilder.highlighter(highlightBuilder);

        //Aggregations
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("by_city").field("city.keyword");
        aggregationBuilder.subAggregation(AggregationBuilders.avg("avg_balance").field("balance"));
        searchSourceBuilder.aggregation(aggregationBuilder);

        //Suggestions 搜索建议
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        TermSuggestionBuilder text = SuggestBuilders.termSuggestion("firstname").text("Hattie");
        suggestBuilder.addSuggestion("name_suggest", text);
        searchSourceBuilder.suggest(suggestBuilder);

        // profile -监控性能消耗
        searchSourceBuilder.profile(true);

        searchRequest.indices("twitter"); //可以传参限制index
        searchRequest.source(searchSourceBuilder);
        //searchRequest.routing("routing");
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN); //TODO https://cloud.tencent.com/developer/article/1444024
        searchRequest.preference("_local");

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //searchResponse
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
            logger.error("ShardSearchFailure,failure={}", failure.toString());
        }

        //HIT -debug it
        SearchHits hits = searchResponse.getHits();

        //highlight
        for (SearchHit hit : hits.getHits()) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (highlightFields.size() != 0) {
                HighlightField highlight = highlightFields.get("firstname");
                Text[] fragments = highlight.fragments();
                String fragmentString = fragments[0].string();
            }
        }

        //Aggregations
        Aggregations aggregations = searchResponse.getAggregations();
        Terms by_city = aggregations.get("by_city");
        Terms.Bucket bucketByKey = by_city.getBucketByKey("Elastic");
        Avg balance = bucketByKey.getAggregations().get("balance");
        double value = balance.getValue();

        Map<String, Aggregation> aggregationsAsMap = aggregations.getAsMap();
        Terms cityAggregation = (Terms) aggregationsAsMap.get("by_city");

        //Suggestion
        Suggest suggest = searchResponse.getSuggest();
        TermSuggestion name_suggest = suggest.getSuggestion("name_suggest");
        name_suggest.getEntries().forEach(entry -> {
            for (TermSuggestion.Entry.Option option : entry) {
                String suggestText = option.getText().string();
                logger.info("suggestText={}", suggestText);
            }
        });

        //Profiling results
        Map<String, ProfileShardResult> profilingResults = searchResponse.getProfileResults();
        for (Map.Entry<String, ProfileShardResult> profilingResult : profilingResults.entrySet()) {
            String key = profilingResult.getKey();
            ProfileShardResult profileShardResult = profilingResult.getValue();
            logger.info("key={},profileShardResult={}", key, profileShardResult);
        }


    }


    /**
     * The Scroll API can be used to retrieve a large number of results from a search request.
     * Search Scroll API
     */
    @Test
    public void test2SearchScrollAPI() throws IOException {
        //1 initialize the search scroll context
        //When processing this SearchRequest, Elasticsearch detects the presence of the scroll parameter and keeps the search context alive for the corresponding time interval.
        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("gender", "M"));
        searchSourceBuilder.size(5);

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L)); //Set the scroll interval

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId(); //
        SearchHits hits = searchResponse.getHits(); //Retrieve the first batch of search hits

        logger.info("scrollId={},hits={}", scrollId, hits);
        //FGluY2x1ZGVfY29udGV4dF91dWlkDXF1ZXJ5QW5kRmV0Y2gBFFBhTDl4WElCWkF1VzJNbGVWUjNTAAAAAAAAC_sWaFVYTFg0d1hSeFN5dEdhTmxvU0lTdw==

    }


    @Test
    public void test2SearchScrollAPI_2() throws IOException {
        //init context
        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("gender", "M"));
        searchSourceBuilder.size(5);

        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L)); //Set the scroll interval，为了使用 scroll，初始搜索请求应该在查询中指定 scroll 参数，告诉 Elasticsearch 需要保持搜索的上下文环境多长时间（滚动时间）

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId1 = searchResponse.getScrollId(); //
        SearchHits hits1 = searchResponse.getHits(); //Retrieve the first batch of search hits
        logger.info("scrollId1={},hits1={}", scrollId1, hits1);


        //scroll
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId1);
        searchScrollRequest.scroll(TimeValue.timeValueSeconds(30));
        SearchResponse searchResponse2 = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
        SearchHits hits2 = searchResponse2.getHits();
        String scrollId2 = searchResponse2.getScrollId();
        TotalHits totalHits2 = hits2.getTotalHits();
        logger.info("scrollId2={},hits2={},totalHits2={}", scrollId2, hits2, totalHits2);
    }


    /**
     * full example of scrolled search
     *
     * @throws IOException
     */
    @Test
    public void test3SearchScrollAPI_FULL_EXAMPLE() throws IOException {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest("twitter");
        searchRequest.scroll(scroll);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("age", "20"));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        int count = 0;
        while (searchHits != null && searchHits.length > 0) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = restHighLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            for (SearchHit searchHit : searchHits) {
                logger.info(searchHit.getSourceAsString());
            }
            logger.info("count={},searchHits={}", count++, searchHits);

        }
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
    }


    /**
     * Clear Scroll API
     */
    @Test
    public void test4ClearScrollAPI() throws IOException {
        final String scrollId = "";
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        int released = clearScrollResponse.getNumFreed(); // Return the number of released search contexts
    }


    /**
     * Multi-Search API
     */
    @Test
    public void test5MultiSearchAPI() throws IOException {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();

        SearchRequest searchRequest1 = new SearchRequest("twitter");
        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1.query(QueryBuilders.matchQuery("gender", "M"));
        searchRequest1.source(searchSourceBuilder1);

        SearchRequest searchRequest2 = new SearchRequest("twitter");
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
        searchSourceBuilder2.query(QueryBuilders.matchQuery("city", "Wollochet"));
        searchRequest2.source(searchSourceBuilder2);

        multiSearchRequest.add(searchRequest1).add(searchRequest2);
        MultiSearchResponse multiSearchResponse = restHighLevelClient.msearch(multiSearchRequest, RequestOptions.DEFAULT);

        MultiSearchResponse.Item firstResponse = multiSearchResponse.getResponses()[0];
        assertNull(firstResponse.getFailure());
        SearchResponse searchResponse = firstResponse.getResponse();
        assertEquals(506, searchResponse.getHits().getTotalHits().value);
        MultiSearchResponse.Item secondResponse = multiSearchResponse.getResponses()[1];
        assertNull(secondResponse.getFailure());
        searchResponse = secondResponse.getResponse();
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
    }

    /**
     * Search Template API
     */
    @Test
    public void test6SearchTemplateAPI() throws IOException {
        SearchRequest searchRequest = new SearchRequest("twitter");
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(searchRequest);
        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setScript("{" +
                "  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                "  \"size\" : \"{{size}}\"" +
                "}");
        HashMap<String, Object> objectHashMap = new HashMap<>();
        objectHashMap.put("field", "city");
        objectHashMap.put("value", "Wollochet");
        objectHashMap.put("size", 5);
        searchTemplateRequest.setScriptParams(objectHashMap);
        searchTemplateRequest.setExplain(true);
        searchTemplateRequest.setProfile(true);
        boolean simulate = false;
        searchTemplateRequest.setSimulate(simulate); // source

        SearchTemplateResponse searchTemplateResponse = restHighLevelClient.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
        logger.info("getTotalHits={}", searchTemplateResponse.getResponse().getHits().getTotalHits());
        if (simulate) {
            logger.info("searchTemplateResponse.getSource()={}", searchTemplateResponse.getSource().utf8ToString()); //when  set Simulate= true;
        }
        //Note that the stored scripts API is not yet available in the high-level REST client, so in this example we use the low-level REST client.
    }

    /**
     * Multi-Search-Template API
     *
     * @throws IOException
     */
    @Test
    public void test7SearchTemplateAPI() throws IOException {
        String[] searchTerms = {"Dominguez", "Marie", "Rosella"};

        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        for (String searchTerm : searchTerms) {
            SearchTemplateRequest request = new SearchTemplateRequest();
            request.setRequest(new SearchRequest("twitter"));

            request.setScriptType(ScriptType.INLINE);
            request.setScript(
                    "{" +
                            "  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                            "  \"size\" : \"{{size}}\"" +
                            "}");

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "firstname");
            scriptParams.put("value", searchTerm);
            scriptParams.put("size", 1);
            request.setScriptParams(scriptParams);
            multiRequest.add(request);
        }

        MultiSearchTemplateResponse multiSearchTemplateResponse = restHighLevelClient.msearchTemplate(multiRequest, RequestOptions.DEFAULT);
        Arrays.stream(multiSearchTemplateResponse.getResponses()).forEach(res -> {
                    if (res.isFailure()) {
                        String error = res.getFailureMessage();
                        logger.error("error={}", error);
                    } else {
                        SearchTemplateResponse searchTemplateResponse = res.getResponse();
                        SearchResponse searchResponse = searchTemplateResponse.getResponse();
                        logger.error("getHits={}", searchResponse.getHits());
                    }
                }
        );
    }

    /**
     * Field Capabilities API
     */
    @Test
    public void test7FieldCapabilitiesAPI() throws IOException {
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest();
        FieldCapabilitiesRequest capabilitiesRequest = fieldCapabilitiesRequest.fields("*name").indices("twitter", "bank");
        capabilitiesRequest.indicesOptions(IndicesOptions.lenientExpandOpen());//index option
        FieldCapabilitiesResponse fieldCapabilitiesResponse = restHighLevelClient.fieldCaps(capabilitiesRequest, RequestOptions.DEFAULT);
        Map<String, Map<String, FieldCapabilities>> stringMapMap = fieldCapabilitiesResponse.get();
        stringMapMap.forEach((key, value) -> {
            logger.info("key={},value={}", key, value);
        });
    }

    /**
     * Ranking Evaluation API
     */
    @Test
    public void test8RankingEvaluationAPI() throws IOException {
        EvaluationMetric metric = new PrecisionAtK();  //Define the metric used in the evaluation 定义评估指标

        List<RatedDocument> ratedDocs = new ArrayList<>();
        ratedDocs.add(new RatedDocument("twitter", "1", 1)); //Add rated documents, specified by index name, id and rating    添加评分文件

        SearchSourceBuilder searchQuery = new SearchSourceBuilder();
        searchQuery.query(QueryBuilders.matchQuery("firstname", "Rosella"));  //Create the search query to evaluate   评估

        RatedRequest ratedRequest = new RatedRequest("name_query", ratedDocs, searchQuery);      //合并三个参数为 RatedRequest
        List<RatedRequest> ratedRequests = Collections.singletonList(ratedRequest);
        RankEvalSpec specification = new RankEvalSpec(ratedRequests, metric);   //Create the ranking evaluation specification

        RankEvalRequest request = new RankEvalRequest(specification, new String[]{"twitter"}); //Create the ranking evaluation request

        RankEvalResponse rankEvalResponse = restHighLevelClient.rankEval(request, RequestOptions.DEFAULT);

        //i still do not understand what the Ranking Evaluation focus on.
        double evaluationResult = rankEvalResponse.getMetricScore();
        assertEquals(1.0 / 3.0, evaluationResult, 0.0);
        Map<String, EvalQueryQuality> partialResults = rankEvalResponse.getPartialResults();
        EvalQueryQuality evalQuality = partialResults.get("name_query");
        assertEquals("name_query", evalQuality.getId());
        double qualityLevel = evalQuality.metricScore();
        assertEquals(1.0 / 3.0, qualityLevel, 0.0);
        List<RatedSearchHit> hitsAndRatings = evalQuality.getHitsAndRatings();
        RatedSearchHit ratedSearchHit = hitsAndRatings.get(2);
        assertEquals("3", ratedSearchHit.getSearchHit().getId());
        assertFalse(ratedSearchHit.getRating().isPresent());
        MetricDetail metricDetails = evalQuality.getMetricDetails();
        String metricName = metricDetails.getMetricName();
        assertEquals(PrecisionAtK.NAME, metricName);
        PrecisionAtK.Detail detail = (PrecisionAtK.Detail) metricDetails;
        assertEquals(1, detail.getRelevantRetrieved());
        assertEquals(3, detail.getRetrieved());

    }

    /**
     * Explain API
     */
    @Test
    public void test8ExplainAPI() throws IOException {
        ExplainRequest request = new ExplainRequest("twitter", "157");
        request.query(QueryBuilders.termQuery("firstname", "Claudia"));
        request.preference("_local");
        request.fetchSourceContext(new FetchSourceContext(true, new String[]{"*s*"}, null));
        ExplainResponse response = restHighLevelClient.explain(request, RequestOptions.DEFAULT);

        String index = response.getIndex();
        String id = response.getId();
        boolean exists = response.isExists();
        boolean match = response.isMatch();
        boolean hasExplanation = response.hasExplanation();
        Explanation explanation = response.getExplanation();
        GetResult getResult = response.getGetResult();

        logger.info("index={},id={},exists={},match={},hasExplanation={},explanation={},getResult={}", index, id, exists, match, hasExplanation, explanation, getResult);

    }

    /**
     * count API
     *
     * @throws IOException
     */
    @Test
    public void test8CountAPI() throws IOException {
        CountRequest countRequest = new CountRequest();
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        countRequest.query(QueryBuilders.matchAllQuery());
        countRequest.indices("twitter").preference("_local").indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        CountResponse countResponse = restHighLevelClient.count(countRequest, RequestOptions.DEFAULT);

        long count = countResponse.getCount();
        RestStatus status = countResponse.status();
        Boolean terminatedEarly = countResponse.isTerminatedEarly();
        logger.info("count={},status={},terminatedEarly={}", count, status, terminatedEarly);


    }

}
