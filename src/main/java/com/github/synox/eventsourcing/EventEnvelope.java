package com.github.synox.eventsourcing;

import lombok.Value;

@Value
public class EventEnvelope<T> {
    long sequenceNr;
    T event;
}
