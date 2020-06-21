package org.example.demo.service;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.rollup.*;
import org.elasticsearch.client.rollup.job.config.DateHistogramGroupConfig;
import org.elasticsearch.client.rollup.job.config.GroupConfig;
import org.elasticsearch.client.rollup.job.config.MetricConfig;
import org.elasticsearch.client.rollup.job.config.RollupJobConfig;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _14RollupAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_14RollupAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;
    /**
     * 汇总作业 （rollup jobs）是一项定期任务，它将来自索引模式指定的索引中的数据进行汇总，然后将其汇总到新的索引中。 汇总索引是紧凑存储数月或数年历史数据以供可视化和报告使用的好方法。
     * 用到rollup的情况是我们有很多的历史数据，而且通常会比较大。通过使用 rollup 功能，我们可以把很多针对大量数据的统计变为针对经过 rollup 后的索引操作，从而使得数据的统计更加有效
     *
     */


    /**
     * Put Rollup Job API todo
     */
    @Test
    public void test1PutRollupJobAPI() throws IOException {

        //group config
        DateHistogramGroupConfig dateHistogram = new DateHistogramGroupConfig("ingest_timestamp", DateHistogramInterval.HOUR, new DateHistogramInterval("7d"), "UTC");

        //TermsGroupConfig terms = new TermsGroupConfig("hostname", "datacenter");
        //HistogramGroupConfig histogram = new HistogramGroupConfig(5L, "load", "net_in", "net_out");
        GroupConfig groups = new GroupConfig(dateHistogram);

        //Metrics config
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add(new MetricConfig("balance", Arrays.asList("min", "max", "sum")));
        //metrics.add(new MetricConfig("age", Arrays.asList("avg", "value_count")));

        RollupJobConfig rollupJobConfig = new RollupJobConfig("test_rollup_job", "restored-bank", "rollup_index_twitter", "0 49-59 17 * * ?", 10, groups, metrics, TimeValue.timeValueSeconds(30));
        PutRollupJobRequest putRollupJobRequest = new PutRollupJobRequest(rollupJobConfig);

        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.rollup().putRollupJob(putRollupJobRequest, RequestOptions.DEFAULT);
        boolean acknowledged = acknowledgedResponse.isAcknowledged();
        logger.info("acknowledged={}", acknowledged);

    }


    /**
     * Start Rollup Job
     */
    @Test
    public void test2StartRollupJob() throws IOException {
        StartRollupJobRequest request = new StartRollupJobRequest("test_rollup_job");
        StartRollupJobResponse startRollupJobResponse = restHighLevelClient.rollup().startRollupJob(request, RequestOptions.DEFAULT);
        boolean acknowledged = startRollupJobResponse.isAcknowledged();
        logger.info("acknowledged={}", acknowledged);

    }

    /**
     *  Stop Rollup Job API
     */

    /**
     * Delete Rollup Job API
     */

    /**
     * Get Rollup Job API
     */
    @Test
    public void test3GetRollupJobAPI() throws IOException {
        GetRollupJobRequest getAll = new GetRollupJobRequest();
        GetRollupJobResponse rollupJob = restHighLevelClient.rollup().getRollupJob(getAll, RequestOptions.DEFAULT);
        List<GetRollupJobResponse.JobWrapper> jobs = rollupJob.getJobs();
        for (GetRollupJobResponse.JobWrapper job : jobs) {
            logger.info("job={}", job.toString());
        }
    }

    /**
     * Rollup Search API
     */
    @Test
    public void test4RollupSearchAPI() throws IOException {

        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder()
                .size(0)
                .aggregation(new MaxAggregationBuilder("max_balance")
                        .field("balance")));
        SearchResponse response = restHighLevelClient.rollup().search(request, RequestOptions.DEFAULT);

        List<Aggregation> aggregations = response.getAggregations().asList();
        aggregations.forEach(aggregation -> {
            ParsedMax parsedMax = (ParsedMax) aggregation;
            String valueAsString = parsedMax.getValueAsString();
            logger.info("max_balance={}", valueAsString);
        });
    }


}
