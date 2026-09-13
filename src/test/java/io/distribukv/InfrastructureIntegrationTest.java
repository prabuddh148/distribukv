package io.distribukv;

import io.distribukv.support.TestCluster;
import io.distribukv.support.TestCluster.Response;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import static io.distribukv.support.TestCluster.eventually;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The same cluster with the full infrastructure stack, against real Kafka, MySQL and Redis
 * containers: MySQL as each node's storage, Kafka as the replication log, Redis heartbeats for
 * failure detection. Skipped automatically when Docker is not available.
 */
@Testcontainers(disabledWithoutDocker = true)
class InfrastructureIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    @Container
    static final MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.4"))
            .withUsername("root")
            .withPassword("test");

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @TempDir
    static Path dataDir;

    private static TestCluster cluster;

    @BeforeAll
    static void startCluster() {
        cluster = new TestCluster(4, dataDir, id -> List.of(
                "--kv.storage.type=mysql",
                "--kv.storage.mysql-url=" + serverUrl(),
                "--kv.storage.mysql-username=root",
                "--kv.storage.mysql-password=test",
                "--kv.kafka.enabled=true",
                "--kv.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--kv.redis.enabled=true",
                "--kv.redis.url=redis://" + redis.getHost() + ":" + redis.getMappedPort(6379)));
        cluster.startAll();
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @BeforeEach
    void allNodesHealthy() {
        cluster.awaitHealthy();
    }

    @Test
    void everyNodeReportsTheFullStack() {
        cluster.nodeIds().forEach(id -> {
            Response status = cluster.send("GET", id, "/cluster/status", null);
            assertThat(status.body().path("stack").path("storage").asText()).isEqualTo("mysql");
            assertThat(status.body().path("stack").path("replicationLog").asText()).isEqualTo("kafka:kv-replication");
            assertThat(status.body().path("stack").path("membership").asText()).isEqualTo("redis");
        });
    }

    @Test
    void writesArePersistedInEachReplicasOwnMySqlDatabase() throws SQLException {
        String key = "mysql:1";
        assertThat(cluster.send("PUT", "node1", "/kv/" + key + "?consistency=STRONG", "persisted").status()).isEqualTo(200);
        List<String> replicas = cluster.replicasOf(key);

        eventually(() -> cluster.nodeIds().forEach(id ->
                assertThat(valueInMySql(id, key)).as("MySQL row on %s", id).isEqualTo(replicas.contains(id) ? "persisted" : null)));
    }

    @Test
    void eventualWritesAreDurableInKafkaAndReachAReplicaEvenAfterTheCoordinatorDies() {
        String key = "kafka:1";
        List<String> replicas = cluster.replicasOf(key);
        String downReplica = replicas.get(0);
        String coordinator = cluster.notIn(replicas);

        cluster.stop(downReplica);
        Response put = cluster.send("PUT", coordinator, "/kv/" + key + "?consistency=EVENTUAL", "from-the-log");
        assertThat(put.status()).isEqualTo(200);
        assertThat(put.text("replicationLog")).startsWith("kafka:kv-replication/");

        // The coordinator's in-memory hint for the down replica dies with the coordinator...
        cluster.stop(coordinator);
        cluster.start(downReplica);

        // ...so the only way the replica can get the write is by consuming the Kafka log.
        eventually(Duration.ofSeconds(60), () -> assertThat(
                cluster.send("GET", downReplica, "/internal/kv/" + key, null).text("value")).isEqualTo("from-the-log"));
        assertThat(cluster.send("GET", downReplica, "/cluster/local", null).body().path("replicationLagMs").asLong())
                .isGreaterThanOrEqualTo(0);
    }

    @Test
    void redisHeartbeatsDetectACrashedNode() {
        String victim = "node4";
        cluster.stop(victim);
        eventually(() -> {
            Response status = cluster.send("GET", "node1", "/cluster/status", null);
            status.body().path("nodes").forEach(node -> {
                if (node.path("id").asText().equals(victim)) {
                    assertThat(node.path("status").asText()).isEqualTo("DOWN");
                }
            });
        });
    }

    private static String serverUrl() {
        return "jdbc:mysql://" + mysql.getHost() + ":" + mysql.getMappedPort(3306) + "/?allowPublicKeyRetrieval=true&useSSL=false";
    }

    private static String valueInMySql(String nodeId, String key) {
        try (Connection connection = DriverManager.getConnection(serverUrl(), "root", "test");
             PreparedStatement query = connection.prepareStatement("SELECT v FROM kv_" + nodeId + ".kv_entries WHERE k = ?")) {
            query.setString(1, key);
            try (ResultSet rows = query.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        } catch (SQLException e) {
            // The node may not have created its database yet.
            return null;
        }
    }
}
