package io.distribukv.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

class StorageEngineTest {

    @TempDir
    Path dir;

    @Test
    void lastWriteWins() throws IOException {
        try (StorageEngine storage = new StorageEngine(dir, false)) {
            assertThat(storage.apply("k", VersionedValue.of("new", 200, "node1"))).isTrue();
            assertThat(storage.apply("k", VersionedValue.of("old", 100, "node2"))).isFalse();
            assertThat(storage.get("k")).get().extracting(VersionedValue::value).isEqualTo("new");
        }
    }

    @Test
    void equalTimestampsBreakTiesByNodeId() throws IOException {
        try (StorageEngine storage = new StorageEngine(dir, false)) {
            storage.apply("k", VersionedValue.of("from-b", 100, "node-b"));
            storage.apply("k", VersionedValue.of("from-a", 100, "node-a"));
            assertThat(storage.get("k")).get().extracting(VersionedValue::value).isEqualTo("from-b");
        }
    }

    @Test
    void tombstonesWinOverOlderWritesAndAreNotCounted() throws IOException {
        try (StorageEngine storage = new StorageEngine(dir, false)) {
            storage.apply("k", VersionedValue.of("v", 100, "node1"));
            storage.apply("k", VersionedValue.deleted(200, "node1"));
            storage.apply("k", VersionedValue.of("late", 150, "node2"));
            assertThat(storage.get("k")).get().extracting(VersionedValue::tombstone).isEqualTo(true);
            assertThat(storage.liveKeyCount()).isZero();
        }
    }

    @Test
    void replaysCommitLogAfterRestart() throws IOException {
        try (StorageEngine storage = new StorageEngine(dir, false)) {
            storage.apply("a", VersionedValue.of("1", 100, "node1"));
            storage.apply("a", VersionedValue.of("2\twith\ttabs\nand newlines", 101, "node1"));
            storage.apply("b", VersionedValue.of("x", 100, "node1"));
            storage.apply("b", VersionedValue.deleted(102, "node1"));
        }
        try (StorageEngine storage = new StorageEngine(dir, true)) {
            assertThat(storage.get("a")).get().extracting(VersionedValue::value).isEqualTo("2\twith\ttabs\nand newlines");
            assertThat(storage.get("b")).get().extracting(VersionedValue::tombstone).isEqualTo(true);
            assertThat(storage.liveKeyCount()).isEqualTo(1);
        }
    }

    @Test
    void compactsLogToOneRecordPerKey() throws IOException {
        try (StorageEngine storage = new StorageEngine(dir, false)) {
            for (int i = 0; i < 100; i++) {
                storage.apply("k", VersionedValue.of("v" + i, i + 1, "node1"));
            }
        }
        new StorageEngine(dir, false).close();
        assertThat(Files.readAllLines(dir.resolve("commit.log"))).hasSize(1);
    }

    @Test
    void skipsTornRecordAtEndOfLog() throws IOException {
        try (StorageEngine storage = new StorageEngine(dir, false)) {
            storage.apply("k", VersionedValue.of("v", 100, "node1"));
        }
        Files.writeString(dir.resolve("commit.log"), "garbage-without-tabs", StandardCharsets.UTF_8,
                StandardOpenOption.APPEND);
        try (StorageEngine storage = new StorageEngine(dir, false)) {
            assertThat(storage.get("k")).get().extracting(VersionedValue::value).isEqualTo("v");
        }
    }
}
