package org.example.demo.service;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _13MigrationAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_13MigrationAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;

    /**
     * Get Deprecation Info
     */
    @Test
    public void test1GetDeprecationInfo() {
        DeprecationInfoRequest deprecationInfoRequest = new DeprecationInfoRequest();

    }

}
