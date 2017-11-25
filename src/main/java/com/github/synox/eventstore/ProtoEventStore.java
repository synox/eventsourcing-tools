package com.github.synox.eventstore;

import com.google.protobuf.GeneratedMessageV3;
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
public class ProtoEventStore<T extends GeneratedMessageV3> implements AutoCloseable {
    private Journal journal;
    private Parser<T> parser;

    public ProtoEventStore(Parser<T> parser, Path dir) throws IOException {
        this.parser = parser;
        Files.createDirectories(dir);
        journal = JournalBuilder.of(dir.toFile()).open();
    }

    @Override
    public void close() throws IOException {
        journal.close();
    }

    public void append(T o) {
        try {
            journal.write(o.toByteArray(), Journal.WriteType.SYNC);
        } catch (IOException e) {
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
            return parser.parseFrom(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
