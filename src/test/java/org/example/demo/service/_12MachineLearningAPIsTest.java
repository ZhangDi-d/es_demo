package org.example.demo.service;

import org.elasticsearch.client.RestHighLevelClient;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _12MachineLearningAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_12MachineLearningAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;


}
