package com.eventaggregator.avro.component;


import avrogeneratedmodel.Activity;
import avrogeneratedmodel.EventRecord;
import avrogeneratedmodel.Subjects;
import com.eventaggregator.EventAggregatorRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.stream.Collectors.groupingBy;

public class StatisticCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(EventAggregatorRunner.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public void calculateDate(List<EventRecord> eventRecords, AvroDataFileWriter avroDataFileWriter) {
        Iterator<Map.Entry<String, Map<Long, List<EventRecord>>>> entryIterator = sortByCityAndType(eventRecords)
                .entrySet()
                .iterator();
        int tempPast7daysCount = 0;
        int tempPast7daysUniqueCount = 0;
        int tempPast30daysCount = 0;
        int tempPast30daysUniqueCount = 0;
        while (entryIterator.hasNext()) {
            Subjects subject = new Subjects();
            subject.activities = new ArrayList<>();
            Map.Entry<String, Map<Long, List<EventRecord>>> next = entryIterator.next();
            String city = next.getKey();
            Map<Long, List<EventRecord>> value = next.getValue();
            Set<Activity> activitySet = new HashSet<>();
            for (var key : value.keySet()) {
                List<EventRecord> eventRecords1 = value.get(key);
                for (EventRecord record : eventRecords1) {
                    LOG.info("Date comparison is started for record: " + record);
                    Activity activity = new Activity();
                    String evType = (String) record.eventType;
                    subject.type = record.eventSubject.subjectType;
                    subject.id = (int) record.eventSubject.id;
                    LocalDate dateForCheck = LocalDate.parse(record.timestamp.toString(), FORMATTER);
                    if (record.userId != null) {
                        if (dateForCheck.plusDays(30).isBefore(LocalDate.now())) {
                            tempPast30daysCount++;
                        }
                        if (dateForCheck.plusDays(7).isBefore(LocalDate.now())) {
                            tempPast7daysCount++;
                        }
                    } else {
                        if (dateForCheck.plusDays(30).isBefore(LocalDate.now())) {
                            tempPast30daysUniqueCount++;
                        }
                        if (dateForCheck.plusDays(7).isBefore(LocalDate.now())) {
                            tempPast7daysUniqueCount++;
                        }
                    }
                    activity.type = evType;
                    activity.past7daysCount = tempPast7daysCount;
                    activity.past30daysCount = tempPast30daysCount;
                    activity.past7daysUniqueCount = tempPast7daysUniqueCount;
                    activity.past30daysUniqueCount = tempPast30daysUniqueCount;
                    Activity singleActivity = activitySet.stream()
                            .filter(s -> evType.contentEquals(s.type))
                            .findAny()
                            .orElse(null);
                    if (singleActivity == null) {
                        LOG.info("Activity is not exist, trying to create a new one");
                        activitySet.add(activity);
                    } else {
                        LOG.info("Activity is exist, trying update it");
                        singleActivity.past7daysCount += tempPast7daysCount;
                        singleActivity.past7daysUniqueCount += tempPast7daysUniqueCount;
                        singleActivity.past30daysCount += tempPast30daysCount;
                        singleActivity.past30daysUniqueCount += tempPast30daysUniqueCount;
                    }
                    tempPast7daysCount = 0;
                    tempPast7daysUniqueCount = 0;
                    tempPast30daysCount = 0;
                    tempPast30daysUniqueCount = 0;
                }
            }
            avroDataFileWriter.toFileWriter(subject, city, activitySet);
        }
    }


    private Map<String, Map<Long, List<EventRecord>>> sortByCityAndType(List<EventRecord> eventRecords) {
        return eventRecords.stream()
                .collect(groupingBy(eventRecord -> eventRecord.get(2).toString(),
                        groupingBy(eventRecord -> eventRecord.eventSubject.id)));
    }

}
