package com.eventaggregator.beam.model;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
public class EventStatistic implements Serializable {
    private List<Subject> subjects;

}
