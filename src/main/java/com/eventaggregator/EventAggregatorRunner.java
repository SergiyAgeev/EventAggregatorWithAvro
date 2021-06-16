package com.eventaggregator;

import com.eventaggregator.avro.model.AvroDataFileWriter;
import com.eventaggregator.avro.model.JsonPackageReader;
import com.eventaggregator.avro.model.StatisticCalculator;

public class EventAggregatorRunner {
    public static void main(String[] args) {
//avro local file generate runner
//        runLocalAvro();

    }

    public static void runLocalAvro() {
        StatisticCalculator statisticCalculator = new StatisticCalculator();
        JsonPackageReader jsonPackageReader = new JsonPackageReader();
        AvroDataFileWriter avroDataFileWriter = new AvroDataFileWriter();
        statisticCalculator.calculateDate(jsonPackageReader.getEventsFromPackage(), avroDataFileWriter);
    }

}
