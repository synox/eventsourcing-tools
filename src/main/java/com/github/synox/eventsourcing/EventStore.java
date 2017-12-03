package com.github.synox.eventsourcing;

import com.google.common.primitives.Longs;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple wrapper around level-db for protobuffer messages.
 *
 * @param <T> the generated Protobuf message class
 */
public class EventStore<T> implements AutoCloseable {
    private static final long NO_OFFSET = 0;
    private Function<byte[], T> parser;
    private Function<T, byte[]> serializer;
    private final DB db;
    private long nextSequenceNr;
    /**
     * Close ressources on close
     */
    private List<Closeable> closables = new ArrayList<>();

    public EventStore(Path dir, Function<byte[], T> parser, Function<T, byte[]> serializer) throws IOException {
        this.parser = parser;
        this.serializer = serializer;

        Options options = new Options();
        options.compressionType(CompressionType.SNAPPY);
        options.verifyChecksums(true);
        options.createIfMissing(true);

        db = JniDBFactory.factory.open(dir.toFile(), options);
        nextSequenceNr = findNextSequenceNr(db);
    }

    private static long findNextSequenceNr(DB db) throws IOException {
        long nextSequenceNr = 0;
        try (DBIterator it = db.iterator()) {
            it.seekToLast();
            if (it.hasNext()) {
                nextSequenceNr = readKey(it.next().getKey()) + 1;
            }
        }
        return nextSequenceNr;
    }


    @Override
    public void close() throws IOException {
        for (Closeable closable : closables) {
            closable.close();
        }
        db.close();
    }

    public long append(T o) {
        long nextId = this.nextSequenceNr++;
        db.put(Longs.toByteArray(nextId), serializer.apply(o));
        return nextId;
    }


    public Stream<EventEnvelope<T>> stream() throws IOException {
        return stream(NO_OFFSET);
    }

    public Stream<EventEnvelope<T>> stream(long nextId) throws IOException {
        DBIterator it = db.iterator();
        closables.add(it);
        it.seek(Longs.toByteArray(nextId));
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
                .map(this::parse);
    }

    private EventEnvelope<T> parse(Map.Entry<byte[], byte[]> entry) {
        return new EventEnvelope<>(readKey(entry.getKey()), parser.apply(entry.getValue()));
    }


    private static long readKey(byte[] key) {
        return Longs.fromByteArray(key);
    }

}
