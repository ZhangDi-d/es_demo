package org.example.demo.service;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.license.GetLicenseRequest;
import org.elasticsearch.client.license.GetLicenseResponse;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _11LicensingAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_11LicensingAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * Get License API
     */
    @Test
    public void test1GetLicenseRequest() throws IOException {
        GetLicenseRequest request = new GetLicenseRequest();
        GetLicenseResponse response = restHighLevelClient.license().getLicense(request, RequestOptions.DEFAULT);
        String licenseDefinition = response.getLicenseDefinition();
        logger.info("licenseDefinition={}", licenseDefinition);
    }

}
