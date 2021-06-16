package com.eventaggregator.avro.component;

import avrogeneratedmodel.EventRecord;
import com.eventaggregator.EventAggregatorRunner;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JsonPackageReader {
    private static final Logger LOG = LoggerFactory.getLogger(EventAggregatorRunner.class);
    private static final File FOLDER = new File("src/main/java/com/eventaggregator/in/");
    private static final List<File> LIST_OF_FILES = List.of(Objects.requireNonNull(FOLDER.listFiles()));

    public static List<EventRecord> getEventsFromPackage() {
        LOG.info(String.format("number of files in package is %d", LIST_OF_FILES.size()));
        List<EventRecord> events = new ArrayList<>();
        for (File file : LIST_OF_FILES) {
            if (file.isFile()) {
                LOG.info("process started for file with name: " + file.getName());
                try {
                    events.add(parseJsonObject(readJsonPackage(file.getPath())));
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
}
