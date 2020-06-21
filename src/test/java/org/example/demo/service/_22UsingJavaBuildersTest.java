package org.example.demo.service;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

//https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-query-builders.html
@FixMethodOrder(NAME_ASCENDING)
public class _22UsingJavaBuildersTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_22UsingJavaBuildersTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void test1BuildingQueries() {
        //Match All
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        MatchAllQueryBuilder matchAllQueryBuilder1 = QueryBuilders.matchAllQuery();

        //Match
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("field", "value");
        MatchQueryBuilder matchQueryBuilder1 = QueryBuilders.matchQuery("field", "value");


        //


    }
}
