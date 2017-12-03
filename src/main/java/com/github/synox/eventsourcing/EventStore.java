package com.github.synox.eventsourcing;

import com.google.common.primitives.Longs;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple wrapper around level-db for protobuffer messages.
 *
 * @param <T> the generated Protobuf message class
 */
public class EventStore<T extends GeneratedMessageV3> implements AutoCloseable {
    public static final long NO_OFFSET = 0;
    private Parser<T> parser;
    private final DB db;
    private long nextSequenceNr;

    public EventStore(Parser<T> parser, Path dir) throws IOException {
        this.parser = parser;

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
        db.close();
    }

    public long append(T o) {
        long nextId = this.nextSequenceNr++;
        db.put(Longs.toByteArray(nextId), o.toByteArray());
        return nextId;
    }


    public Stream<EventEnvelope<T>> stream() throws IOException {
        return stream(NO_OFFSET);
    }

    public Stream<EventEnvelope<T>> stream(long nextId) throws IOException {
        DBIterator it = db.iterator();
        it.seek(Longs.toByteArray(nextId));
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
                .map(this::parse);
        // TODO: close iterator
    }

    private EventEnvelope<T> parse(Map.Entry<byte[], byte[]> entry) {
        try {
            return new EventEnvelope<>(readKey(entry.getKey()), parser.parseFrom(entry.getValue()));
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }


    private static long readKey(byte[] key) {
        return Longs.fromByteArray(key);
    }

}
