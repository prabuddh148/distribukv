package io.distribukv.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;

/**
 * Per-node storage in MySQL. Each node owns its own database ({@code kv_<nodeId>}), so the
 * database plays the role of a replica's local disk: replication stays in the KV layer.
 *
 * <p>Last-write-wins is enforced inside MySQL, atomically per row, so concurrent writes to the
 * same key (from a coordinator, a hint and the Kafka log at once) can never regress a version.
 */
public final class MySqlStorage implements StorageEngine {

    private final HikariDataSource dataSource;
    private final JdbcTemplate jdbc;
    private final String table;

    public MySqlStorage(String serverUrl, String username, String password, String nodeId) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(serverUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setPoolName("kv-mysql-" + nodeId);
        config.setMaximumPoolSize(16);
        config.setConnectionTimeout(3_000);
        config.setInitializationFailTimeout(60_000); // wait for MySQL to come up instead of crashing
        this.dataSource = new HikariDataSource(config);
        this.jdbc = new JdbcTemplate(dataSource);

        String database = "kv_" + nodeId.replaceAll("[^A-Za-z0-9_]", "_");
        this.table = database + ".kv_entries";
        jdbc.execute("CREATE DATABASE IF NOT EXISTS " + database);
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS %s (
                  k         VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
                  v         MEDIUMTEXT,
                  ts        BIGINT NOT NULL,
                  node_id   VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                  tombstone BOOLEAN NOT NULL
                )""".formatted(table));
    }

    @Override
    public boolean apply(String key, VersionedValue value) {
        if (updateIfNewer(key, value)) {
            return true;
        }
        try {
            jdbc.update("INSERT INTO " + table + " (k, v, ts, node_id, tombstone) VALUES (?, ?, ?, ?, ?)",
                    key, value.value(), value.timestamp(), value.nodeId(), value.tombstone());
            return true;
        } catch (DuplicateKeyException e) {
            // The row exists (possibly inserted concurrently). Rows are never deleted, so one more
            // conditional update gives the final answer.
            return updateIfNewer(key, value);
        }
    }

    private boolean updateIfNewer(String key, VersionedValue value) {
        return jdbc.update("UPDATE " + table + " SET v = ?, ts = ?, node_id = ?, tombstone = ? "
                        + "WHERE k = ? AND (ts < ? OR (ts = ? AND node_id < ?))",
                value.value(), value.timestamp(), value.nodeId(), value.tombstone(),
                key, value.timestamp(), value.timestamp(), value.nodeId()) == 1;
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        return jdbc.query("SELECT v, ts, node_id, tombstone FROM " + table + " WHERE k = ?",
                (rs, row) -> new VersionedValue(rs.getString("v"), rs.getLong("ts"),
                        rs.getString("node_id"), rs.getBoolean("tombstone")),
                key).stream().findFirst();
    }

    @Override
    public long liveKeyCount() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM " + table + " WHERE tombstone = FALSE", Long.class);
        return count == null ? 0 : count;
    }

    @Override
    public String type() {
        return "mysql";
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
