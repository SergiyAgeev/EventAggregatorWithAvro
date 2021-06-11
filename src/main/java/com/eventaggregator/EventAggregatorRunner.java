package com.eventaggregator;

import com.eventaggregator.model.EventRecord.EventRecord;
import com.eventaggregator.model.Subjects.Subjects;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class EventAggregatorRunner {
    private static final Logger LOG = LoggerFactory.getLogger(EventAggregatorRunner.class);
    private static final File FOLDER = new File("src/main/resources");
    private static final List<File> LIST_OF_FILES = List.of(Objects.requireNonNull(FOLDER.listFiles()));
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

    public static void main(String[] args) throws Exception {
        List<EventRecord> eventsFromPackage = getEventsFromPackage();
//        writeToFile(eventsFromPackage);
        Map<String, List<EventRecord>> map = sortByCity(eventsFromPackage);
        List<Subjects> subjects = parseTimestamp(map);
        System.out.println(map);

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

    ///NOK
    public static void writeToFile(List<Subjects> subjectsList) throws IOException {
        final DataFileWriter<Subjects> writer = new DataFileWriter<>(new SpecificDatumWriter<>(Subjects.class));
        LOG.info("events count: " + subjectsList.size());
        File tmpFile = File.createTempFile("events", ".avro");
        writer.create(EventRecord.SCHEMA$, tmpFile);
        for (Subjects singleSubject : subjectsList) {
            LOG.info("writing event: " + singleSubject.toString());
            writer.append(singleSubject);
        }
        writer.close();
        LOG.info("DONE!, file was wrote with name " + tmpFile.getName());
    }

    private static List<Subjects> EventsToSubjectsConvert(List<EventRecord> eventRecords) {
        Map<String, String> someMap = new HashMap();
        eventRecords.stream().map(er -> er.city).collect(Collectors.toSet());

        return null;
    }

    //GOOD
    private static Map<String, List<EventRecord>> sortByCity(List<EventRecord> eventRecords) {
        return eventRecords.stream()
                .collect(groupingBy(eventRecord -> eventRecord.get(2).toString()));
    }

    //IN PROGRESS
    private static List<Subjects> parseTimestamp(Map<String, List<EventRecord>> map) throws Exception {
        Set<String> keys = new HashSet<>();
        if (!map.isEmpty()) {
            keys = map.keySet();
        } else {
            throw new Exception("Map is empty!!");
        }
        for (String singleKey : keys) {
            int tempPast7daysCount = 0;
            int tempPast7daysUniqueCount = 0;
            int tempPast30daysCount = 0;
            int tempPast30daysUniqueCount = 0;

            List<EventRecord> eventRecords = map.get(singleKey);
            for (EventRecord singleEventRecord : eventRecords) {
                if (singleEventRecord.userId != 0) {
                    LocalDate dateTime = LocalDate.parse(singleEventRecord.timestamp.toString(), FORMATTER);
//                    LocalDate dateTime = LocalDate.parse(singleEventRecord.timestamp.toString());
                    if (dateTime.plusDays(30).isBefore(LocalDate.now())){
                        tempPast30daysCount++;
                        if (dateTime.plusDays(7).isBefore(LocalDate.now())){
                            tempPast7daysCount++;
                        }
                    }

                }

            }

        }

        return null;
    }

}
