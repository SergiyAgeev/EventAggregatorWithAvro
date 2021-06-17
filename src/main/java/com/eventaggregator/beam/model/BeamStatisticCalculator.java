package com.eventaggregator.beam.model;

import com.eventaggregator.beam.model.Subject.Activity;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BeamStatisticCalculator extends DoFn<KV<String, Iterable<EventRecord>>, KV<String, EventStatistic>> {
    private static final Logger LOG = LoggerFactory.getLogger(BeamStatisticCalculator.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<EventRecord>> cityEvent, OutputReceiver<KV<String, EventStatistic>> receiver) {
        Map<EventRecord.EventSubject, List<EventRecord>> eventSubjectListMap = StreamSupport
                .stream(Objects.requireNonNull(cityEvent.getValue()).spliterator(), false)
                .collect(Collectors.groupingBy(EventRecord::getEventSubject));

        List<Subject> subjectList = eventSubjectListMap.keySet().stream()
                .map(eventSubject -> {
                    List<EventRecord> eventRecords = eventSubjectListMap.get(eventSubject);
                    Map<String, List<EventRecord>> eventActivityByEventType = eventRecords.stream()
                            .collect(Collectors.groupingBy(EventRecord::getEventType));
                    List<Activity> activities = eventActivityByEventType.entrySet().stream()
                            .map(subject -> {
                                int past7daysCount;
                                int past7daysUniqueCount;
                                int past30daysCount;
                                int past30daysUniqueCount;

                                past7daysCount = Math.toIntExact(subject.getValue().stream()
                                        .filter(eventRecord -> LocalDate.parse(eventRecord.getTimestamp(),
                                                FORMATTER).plusDays(7).isBefore(LocalDate.now()))
                                        .count());
                                past7daysUniqueCount = Math.toIntExact(subject.getValue().stream()
                                        .filter(eventRecord -> LocalDate.parse(eventRecord.getTimestamp(),
                                                FORMATTER).plusDays(7).isBefore(LocalDate.now())
                                                && eventRecord.getUserId() != null)
                                        .count());
                                past30daysCount = Math.toIntExact(subject.getValue().stream()
                                        .filter(eventRecord -> LocalDate.parse(eventRecord.getTimestamp(),
                                                FORMATTER).plusDays(30).isBefore(LocalDate.now()))
                                        .count());
                                past30daysUniqueCount = Math.toIntExact(subject.getValue().stream()
                                        .filter(eventRecord -> LocalDate.parse(eventRecord.getTimestamp(),
                                                FORMATTER).plusDays(30).isBefore(LocalDate.now())
                                                && eventRecord.getUserId() != null)
                                        .count());

                                return new Activity.ActivityBuilder()
                                        .type(subject.getKey())
                                        .past7daysCount(past7daysCount)
                                        .past7daysUniqueCount(past7daysUniqueCount)
                                        .past30daysCount(past30daysCount)
                                        .past30daysUniqueCount(past30daysUniqueCount)
                                        .build();
                            })
                            .collect(Collectors.toList());

                    return new Subject.SubjectBuilder()
                            .id((int) eventSubject.getId())
                            .type(eventSubject.getSubjectType())
                            .activities(activities)
                            .build();
                })
                .collect(Collectors.toList());

        receiver.output(KV.of(cityEvent.getKey(),
                new EventStatistic.EventStatisticBuilder()
                        .subjects(subjectList)
                        .build()));
    }

}
