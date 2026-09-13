package io.distribukv.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-node storage: an in-memory map backed by an append-only commit log.
 *
 * <p>Every accepted write is appended to {@code commit.log} before the call returns. On startup
 * the log is replayed (applying last-write-wins, so record order does not matter and a torn final
 * line is simply skipped) and then compacted to one record per key.
 */
public final class StorageEngine implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(StorageEngine.class);
    private static final Base64.Encoder B64 = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64D = Base64.getUrlDecoder();

    private final Map<String, VersionedValue> data = new ConcurrentHashMap<>();
    private final Path logFile;
    private final boolean fsync;
    private final FileChannel channel;

    public StorageEngine(Path directory, boolean fsync) throws IOException {
        Files.createDirectories(directory);
        this.logFile = directory.resolve("commit.log");
        this.fsync = fsync;
        replay();
        compact();
        this.channel = FileChannel.open(logFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    /**
     * Applies the write if it is newer than what is stored.
     *
     * @return true if the write was applied (false means a newer version already exists)
     */
    public boolean apply(String key, VersionedValue value) {
        boolean[] applied = {false};
        data.compute(key, (k, current) -> {
            if (value.isNewerThan(current)) {
                applied[0] = true;
                return value;
            }
            return current;
        });
        if (applied[0]) {
            append(key, value);
        }
        return applied[0];
    }

    /** The stored version, including tombstones. */
    public Optional<VersionedValue> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    public long liveKeyCount() {
        return data.values().stream().filter(v -> !v.tombstone()).count();
    }

    private void append(String key, VersionedValue value) {
        byte[] line = encode(key, value).getBytes(StandardCharsets.UTF_8);
        synchronized (channel) {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(line);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                if (fsync) {
                    channel.force(false);
                }
            } catch (IOException e) {
                throw new StorageException("Failed to append to commit log", e);
            }
        }
    }

    private void replay() throws IOException {
        if (!Files.exists(logFile)) {
            return;
        }
        List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
        int skipped = 0;
        for (String line : lines) {
            try {
                String[] parts = line.split("\t", -1);
                String key = new String(B64D.decode(parts[0]), StandardCharsets.UTF_8);
                boolean tombstone = "1".equals(parts[3]);
                String value = tombstone ? null : new String(B64D.decode(parts[4]), StandardCharsets.UTF_8);
                VersionedValue version = new VersionedValue(value, Long.parseLong(parts[1]), parts[2], tombstone);
                data.merge(key, version, (current, candidate) -> candidate.isNewerThan(current) ? candidate : current);
            } catch (RuntimeException e) {
                skipped++;
            }
        }
        log.info("Replayed {} commit log records into {} keys ({} corrupt records skipped)",
                lines.size(), data.size(), skipped);
    }

    private void compact() throws IOException {
        Path tmp = logFile.resolveSibling("commit.log.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, VersionedValue> entry : data.entrySet()) {
                writer.write(encode(entry.getKey(), entry.getValue()));
            }
        }
        Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String encode(String key, VersionedValue v) {
        String value = v.tombstone() ? "" : B64.encodeToString(v.value().getBytes(StandardCharsets.UTF_8));
        return B64.encodeToString(key.getBytes(StandardCharsets.UTF_8)) + '\t' + v.timestamp() + '\t'
                + v.nodeId() + '\t' + (v.tombstone() ? '1' : '0') + '\t' + value + '\n';
    }

    @Override
    public void close() throws IOException {
        synchronized (channel) {
            channel.force(false);
            channel.close();
        }
    }

    public static final class StorageException extends RuntimeException {
        public StorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
