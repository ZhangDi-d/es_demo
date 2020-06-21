package org.example.demo.service;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.xpack.XPackInfoRequest;
import org.elasticsearch.client.xpack.XPackInfoResponse;
import org.elasticsearch.client.xpack.XPackUsageRequest;
import org.elasticsearch.client.xpack.XPackUsageResponse;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _4MiscellaneousAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_4MiscellaneousAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * indo api
     */
    @Test
    public void test1infoAPI() throws IOException {
        MainResponse response = restHighLevelClient.info(RequestOptions.DEFAULT);
        logger.info("info={}", response);

        String clusterName = response.getClusterName();
        String clusterUuid = response.getClusterUuid();
        String nodeName = response.getNodeName();
        MainResponse.Version version = response.getVersion();
        String buildDate = version.getBuildDate();
        String buildFlavor = version.getBuildFlavor();
        String buildHash = version.getBuildHash();
        String buildType = version.getBuildType();
        String luceneVersion = version.getLuceneVersion();
        String minimumIndexCompatibilityVersion = version.getMinimumIndexCompatibilityVersion();
        String minimumWireCompatibilityVersion = version.getMinimumWireCompatibilityVersion();
        String number = version.getNumber();
        logger.info("clusterName={},clusterUuid={},nodeName={},buildDate={},buildFlavor={},buildHash={},buildType={},luceneVersion={},minimumIndexCompatibilityVersion={},minimumWireCompatibilityVersion={},number={}",
                clusterName, clusterUuid, nodeName, buildDate, buildFlavor, buildHash, buildType, luceneVersion, minimumIndexCompatibilityVersion, minimumWireCompatibilityVersion, number);
    }

    /**
     * PING API
     */
    @Test
    public void test2PingAPI() throws IOException {
        boolean response = restHighLevelClient.ping(RequestOptions.DEFAULT);
        logger.info("response={}", response);
    }

    /**
     * X-Pack Info API
     * X-Pack Usage API
     */
    @Test
    public void test3XPACKInfoAPI() throws IOException {
        XPackInfoRequest xPackInfoRequest = new XPackInfoRequest();
        xPackInfoRequest.setVerbose(true);
        xPackInfoRequest.setCategories(EnumSet.of(
                XPackInfoRequest.Category.BUILD,
                XPackInfoRequest.Category.LICENSE,
                XPackInfoRequest.Category.FEATURES));

        XPackInfoResponse response = restHighLevelClient.xpack().info(xPackInfoRequest, RequestOptions.DEFAULT);

        XPackInfoResponse.BuildInfo build = response.getBuildInfo();
        XPackInfoResponse.LicenseInfo license = response.getLicenseInfo();
        XPackInfoResponse.FeatureSetsInfo features = response.getFeatureSetsInfo();

        XPackUsageRequest request = new XPackUsageRequest();
        XPackUsageResponse response2 = restHighLevelClient.xpack().usage(request, RequestOptions.DEFAULT);
        Map<String, Map<String, Object>> usages = response2.getUsages();
        Map<String, Object> monitoringUsage = usages.get("monitoring");

    }


}
