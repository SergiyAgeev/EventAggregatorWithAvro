package com.eventaggregator.beam.model;

import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

import java.io.Serializable;
import java.util.List;

@Data
@Builder
@DefaultSchema(JavaBeanSchema.class)
public class EventStatistic implements Serializable {
    private List<Subject> subjects;

    @SchemaCreate
    public EventStatistic(List<Subject> subjects) {
        this.subjects = subjects;
    }
}
