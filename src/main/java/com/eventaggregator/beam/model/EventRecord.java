package com.eventaggregator.beam.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class EventRecord implements Serializable {
    private long id;
    private Long userId;
    private String city;
    private String eventType;
    private String timestamp;
    private EventSubject eventSubject;

    @Data
    @Builder
    static class EventSubject implements Serializable {
        private long id;
        private String subjectType;
    }

}
