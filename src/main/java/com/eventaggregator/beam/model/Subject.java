package com.eventaggregator.beam.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class Subject implements Serializable {
    private int id;
    private String type;
    private List<Activity> activities;

    @Data
    @Builder
    static class Activity implements Serializable {
        private String type;
        private int past7daysCount;
        private int past7daysUniqueCount;
        private int past30daysCount;
        private int past30daysUniqueCount;
    }

}
