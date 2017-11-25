package com.github.synox.eventstore;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import journal.io.api.Journal;
import journal.io.api.JournalBuilder;
import journal.io.api.Location;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple wrapper around journal.io for protobuffer messages.
 *
 * @param <T> the generated Protobuf message class
 */
public class ProtoEventStore<T extends GeneratedMessageV3> {
    private Journal journal;
    private Parser<T> parser;
    private Path dir;

    public ProtoEventStore(Parser<T> parser, Path dir) {
        this.parser = parser;
        this.dir = dir;
    }

    public void start() throws IOException {
        Files.createDirectories(dir);
        journal = JournalBuilder.of(dir.toFile()).open();
    }

    public void stop() throws IOException {
        journal.close();
    }

    public void append(T o) {
        try {
            journal.write(serialize(o), Journal.WriteType.SYNC);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] serialize(T o) {
        return o.toByteArray();
    }

    private T deserialize(byte[] bytes) {
        try {
            return parser.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public Stream<T> stream() throws IOException {
        return StreamSupport.stream(journal.redo().spliterator(), false)
                .map(this::readLocation);
    }

    private T readLocation(Location loc) {
        try {
            byte[] record = journal.read(loc, Journal.ReadType.SYNC);
            return deserialize(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
