package com.eventaggregator;

import com.eventaggregator.model.EventRecord.EventRecord;
import com.eventaggregator.model.Subjects.Activity;
import com.eventaggregator.model.Subjects.Subjects;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.stream.Collectors.groupingBy;

public class EventAggregatorRunner {
    private static final Logger LOG = LoggerFactory.getLogger(EventAggregatorRunner.class);
    private static final File FOLDER = new File("src/main/resources");
    private static final List<File> LIST_OF_FILES = List.of(Objects.requireNonNull(FOLDER.listFiles()));
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public static void main(String[] args) throws Exception {
        calculateDateAndSaveIntoAvroFile(getEventsFromPackage());
    }

    public static List<EventRecord> getEventsFromPackage() throws Exception {
        LOG.info(String.format("number of files in package is %d", LIST_OF_FILES.size()));
        List<EventRecord> events = new ArrayList<>();
        for (File file : LIST_OF_FILES) {
            if (file.isFile()) {
                LOG.info("process started for file with name: " + file.getName());
                events.add(parseJsonObject(readJsonPackage(file.getPath())));
            }
        }
        return events;
    }

    private static JSONObject readJsonPackage(String filename) throws Exception {
        FileReader reader = new FileReader(filename);
        JSONParser jsonParser = new JSONParser();
        return (JSONObject) jsonParser.parse(reader);
    }

    private static EventRecord parseJsonObject(JSONObject jsonObject) throws IOException {
        LOG.info("json to avro parse is started");
        EventRecord event = new ObjectMapper().readValue(jsonObject.toString(), EventRecord.class);
        LOG.info("event parse finished for event: " + event);
        return event;
    }

    private static Map<String, Map<Long, List<EventRecord>>> sortByCityAndType(List<EventRecord> eventRecords) {
        return eventRecords.stream()
                .collect(groupingBy(eventRecord -> eventRecord.get(2).toString(),
                        groupingBy(eventRecord -> eventRecord.eventSubject.id))
                );
    }

    public static void calculateDateAndSaveIntoAvroFile(List<EventRecord> eventRecords) throws IOException {
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
                        activitySet.add(activity);

                    } else {
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

            final DataFileWriter<Subjects> writer = new DataFileWriter<>(new SpecificDatumWriter<>(Subjects.class));
            File tmpFile = File.createTempFile(city, ".avro");
            LOG.info("trying to save AVRO file with name: " + tmpFile.getName() + ", in package: " +tmpFile.getPath());
            writer.create(Subjects.SCHEMA$, tmpFile);
            subject.activities = new ArrayList<>(activitySet);
            writer.append(subject);
            writer.close();
            LOG.info("DONE! AVRO file was created for subject: " + subject);
        }
    }

}
