package com.eventaggregator;

import com.eventaggregator.avro.component.AvroDataFileWriter;
import com.eventaggregator.avro.component.JsonPackageReader;
import com.eventaggregator.avro.component.StatisticCalculator;


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
