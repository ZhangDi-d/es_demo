package org.example.demo.service;

import org.elasticsearch.client.RestHighLevelClient;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

/**
 * 简单地说：您可以使用enrich processor在ingest期间将现有索引中的数据添加到传入文档中。比如，你可以在如下的场景中用到：
 * <p>
 * 根据已知的IP地址识别Web服务或供应商
 * 根据产品ID将产品信息添加到零售订单
 * 根据电子邮件地址补充联系信息
 * 根据用户坐标添加邮政编码
 */
@FixMethodOrder(NAME_ASCENDING)
public class _21EnrichAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_21EnrichAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;
}
