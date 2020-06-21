package org.example.demo.service;

import io.searchbox.client.JestClient;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@RunWith(SpringRunner.class)
@SpringBootTest
public class JestTest {
    @Resource
    private JestClient jestClient;

    @Test
    public void index() {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("number_of_shards", 5);
        settingsBuilder.put("number_of_replicas", 0);

    }
}
