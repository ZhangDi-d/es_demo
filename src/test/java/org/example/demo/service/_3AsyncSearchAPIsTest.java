package org.example.demo.service;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _3AsyncSearchAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_3AsyncSearchAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;


    /**
     * Submit Async Search API
     * Get Async Search API
     * Delete Async Search API
     */
    @Test
    public void Test1SubmitAsyncSearchAPI() {
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery());
        String[] indices = new String[]{"twitter"};
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchSource, indices);

        request.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        request.setKeepAlive(TimeValue.timeValueMinutes(15));
        request.setKeepOnCompletion(false);

        //7.6.2似乎没有
        //AsyncSearchResponse response = restHighLevelClient.asyncSearch().submit(request, RequestOptions.DEFAULT);
        //DeleteAsyncSearchRequest request = new DeleteAsyncSearchRequest(id);
    }


}
