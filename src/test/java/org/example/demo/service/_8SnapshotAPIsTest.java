package org.example.demo.service;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.example.demo.AbstractTest;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(NAME_ASCENDING)
public class _8SnapshotAPIsTest extends AbstractTest {
    private final static Logger logger = LoggerFactory.getLogger(_8SnapshotAPIsTest.class);
    @Resource
    private RestHighLevelClient restHighLevelClient;
    /**
     * Elasticsearch 做备份有两种方式，一是将数据导出成文本文件，比如通过 elasticdump、esm 等工具将存储在 Elasticsearch 中的数据导出到文件中。
     * 二是以备份 elasticsearch data 目录中文件的形式来做快照，也就是 Elasticsearch 中 snapshot 接口实现的功能。
     */

    /**
     * Snapshot Get Repository API
     */
    @Test
    public void test1SnapshotGetRepositoryAPI() throws IOException {
        GetRepositoriesRequest getRepositoriesRequest = new GetRepositoriesRequest();
        String[] repositories = new String[]{"repo_twitter"};
        getRepositoriesRequest.repositories(repositories);
        GetRepositoriesResponse getRepositoriesResponse = restHighLevelClient.snapshot().getRepository(getRepositoriesRequest, RequestOptions.DEFAULT);
        getRepositoriesResponse.repositories().forEach(a -> {
            logger.info("name={}", a.name());
        });
    }

    /**
     * Snapshot Create RepositoryAPI
     * <p>
     * 需要在elasticsearch.yml中配置 path.repo: ["D:\\var\\testdata\\data"]
     */
    @Test
    public void test2SnapshotCreateRepositoryAPI() throws IOException {
        PutRepositoryRequest putRepositoryRequest = new PutRepositoryRequest();
        String locationKey = FsRepository.LOCATION_SETTING.getKey();
        String locationValue = "D:\\var\\testdata\\data";
        String compressKey = FsRepository.COMPRESS_SETTING.getKey();
        boolean compressValue = true;
        Settings.Builder builder = Settings.builder().put(locationKey, locationValue).put(compressKey, compressValue);
        putRepositoryRequest.settings(builder);
        putRepositoryRequest.name("repo_twitter");
        putRepositoryRequest.type(FsRepository.TYPE);
        AcknowledgedResponse response = restHighLevelClient.snapshot().createRepository(putRepositoryRequest, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        logger.info("acknowledged={}", acknowledged);

    }


    /**
     * Snapshot Verify Repository API
     */
    @Test
    public void test3SnapshotVerifyRepositoryAPI() throws IOException {
        VerifyRepositoryRequest request = new VerifyRepositoryRequest("repo_twitter");
        VerifyRepositoryResponse response = restHighLevelClient.snapshot().verifyRepository(request, RequestOptions.DEFAULT);
        List<VerifyRepositoryResponse.NodeView> nodes = response.getNodes();
        for (VerifyRepositoryResponse.NodeView node : nodes) {
            logger.info("node={}", node.getName());
        }
    }

    /**
     * Create Snapshot API
     */
    @Test
    public void test4CreateSnapshotAPI() throws IOException {
        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest();
        createSnapshotRequest.repository("repo_twitter");
        createSnapshotRequest.snapshot("twitter_snapshot_0001");

        createSnapshotRequest.indices("twitter");
        createSnapshotRequest.indicesOptions(IndicesOptions.fromOptions(false, false, true, true));

        createSnapshotRequest.partial(false);//设为true意味着 创建一个快照不需要所有分片都可用 ；Set partial to true to allow a successful snapshot without the availability of all the indices primary shards. Defaults to false.
        createSnapshotRequest.includeGlobalState(true); //将includeGlobalState设置为false可以防止将群集的全局状态写入快照。默认为true。

        createSnapshotRequest.masterNodeTimeout("1m");
        createSnapshotRequest.waitForCompletion(true);

        CreateSnapshotResponse response = restHighLevelClient.snapshot().create(createSnapshotRequest, RequestOptions.DEFAULT);
        RestStatus status = response.status();
        logger.info("status={}", status);
    }

    /**
     * Get Snapshots API
     */
    @Test
    public void test5GetSnapshotsAPI() throws IOException {
        GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest();
        getSnapshotsRequest.repository("repo_twitter");
        getSnapshotsRequest.snapshots(new String[]{"twitter_snapshot_0001"});
        getSnapshotsRequest.ignoreUnavailable(true);
        getSnapshotsRequest.masterNodeTimeout("30s");
        GetSnapshotsResponse getSnapshotsResponse = restHighLevelClient.snapshot().get(getSnapshotsRequest, RequestOptions.DEFAULT);
        getSnapshotsResponse.getSnapshots().forEach(snapshotInfo -> {
            SnapshotInfo basic = snapshotInfo.basic();
            logger.info("basic={}", basic.toString());
        });
    }

    /**
     * Snapshots Status API
     */

    /**
     * Delete Snapshot API
     */
    @Test
    public void test6DeleteSnapshotAPI() throws IOException {
        DeleteSnapshotRequest deleteSnapshotRequest = new DeleteSnapshotRequest();
        deleteSnapshotRequest.repository("repo_twitter");
        deleteSnapshotRequest.snapshot("twitter_snapshot_0001");
        deleteSnapshotRequest.masterNodeTimeout("10s");
        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.snapshot().delete(deleteSnapshotRequest, RequestOptions.DEFAULT);
        boolean acknowledged = acknowledgedResponse.isAcknowledged();
        logger.info("acknowledged={}", acknowledged);
    }

    /**
     * Restore Snapshot API  这里是将快照恢复成数据的API，正所谓养兵千日，用兵一时
     */
    @Test
    public void test7RestoreSnapshotAPI() throws IOException {
        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest();
        restoreSnapshotRequest.indices("twitter"); //With the indices property you can provide a list of indices that should be restored:

        restoreSnapshotRequest.renamePattern("twitter"); //待修改的命名
        restoreSnapshotRequest.renameReplacement("restored_twitter"); //renaming 的过程 可以正则

        restoreSnapshotRequest.repository("repo_twitter");
        restoreSnapshotRequest.snapshot("twitter_snapshot_0001");

        restoreSnapshotRequest.indexSettings(
                Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .build());

        restoreSnapshotRequest.ignoreIndexSettings("index.refresh_interval", "index.search.idle.after");
        restoreSnapshotRequest.indicesOptions(new IndicesOptions(EnumSet.of(IndicesOptions.Option.IGNORE_UNAVAILABLE), EnumSet.of(IndicesOptions.WildcardStates.OPEN)));

        restoreSnapshotRequest.waitForCompletion(true);
        restoreSnapshotRequest.partial(false);
        restoreSnapshotRequest.masterNodeTimeout("30s");

        restoreSnapshotRequest.includeGlobalState(true);//模板
        restoreSnapshotRequest.includeAliases(false);//别名

        RestoreSnapshotResponse response = restHighLevelClient.snapshot().restore(restoreSnapshotRequest, RequestOptions.DEFAULT);
        RestoreInfo restoreInfo = response.getRestoreInfo();
        logger.info("restoreInfo={}", restoreInfo.toString());

    }


}
