package org.example.demo.service;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _9TasksAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_9TasksAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * List Tasks API
     */
    @Test
    public void test1ListTasksAPI() throws IOException {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setDetailed(true);
        listTasksRequest.setWaitForCompletion(true);
        listTasksRequest.setTimeout(TimeValue.timeValueSeconds(50));
        listTasksRequest.setTimeout("50s");

        ListTasksResponse listTasksResponse = restHighLevelClient.tasks().list(listTasksRequest, RequestOptions.DEFAULT);
        logger.info("listTasksResponse={}", listTasksResponse.toString());
    }


    /**
     * Cancel Tasks API
     */

}
