package com.eventaggregator.avro.model;

import avrogeneratedmodel.Activity;
import avrogeneratedmodel.Subjects;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

public class AvroDataFileWriter {
    private static final Logger LOG = LoggerFactory.getLogger(AvroDataFileWriter.class);

    public void toFileWriter(Subjects subject, String city, Set<Activity> activitySet) {
        try (DataFileWriter<Subjects> writer = new DataFileWriter<>(new SpecificDatumWriter<>(Subjects.class))) {
            File file = new File(
                    String.format("src/main/java/com/eventaggregator/out/%s %s %d.avro", city, LocalDate.now(),
                            new Random().nextInt()));
            LOG.info(String.format("trying to save AVRO file with name: %s, in package: %s", file.getName(),
                    file.getPath()));
            writer.create(Subjects.SCHEMA$, file);
            subject.activities = new ArrayList<>(activitySet);
            writer.append(subject);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info(String.format("DONE! AVRO file was created for subject: %s", subject));
    }

}
