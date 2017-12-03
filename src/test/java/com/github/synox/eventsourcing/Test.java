package com.github.synox.eventsourcing;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Test {

    private EventStore<String> store;

    @org.junit.jupiter.api.Test
    void streamNoOffset() throws IOException {
        assertEquals(Arrays.asList(
                new EventEnvelope<>(0, "hello1"),
                new EventEnvelope<>(1, "hello2")),
                store.stream().collect(Collectors.toList()));
    }

    @org.junit.jupiter.api.Test
    void streamOffset() throws IOException {
        assertEquals(Arrays.asList(
                new EventEnvelope<>(1, "hello2")),
                store.stream(1).collect(Collectors.toList()));
    }

    private static void deleteDir(Path rootPath) throws IOException {
        if (Files.exists(rootPath)) {
            Files.walk(rootPath)
                    .map(Path::toFile)
                    .forEach(File::delete);
            Files.deleteIfExists(rootPath);
        }
    }


    @BeforeEach
    void setupDb() throws IOException {
        Path db = Paths.get("target/test.leveldb");
        deleteDir(db);

        Function<byte[], String> parser = String::new;
        Function<String, byte[]> serializer = String::getBytes;
        store = new EventStore<String>(db, parser, serializer);
        store.append("hello1");
        store.append("hello2");
    }

    @AfterEach
    void close() throws IOException {
        store.close();
    }


}