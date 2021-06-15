package com.eventaggregator;

import com.eventaggregator.avro.component.AvroDataFileWriter;
import com.eventaggregator.avro.component.JsonPackageReader;
import com.eventaggregator.avro.component.StatisticCalculator;

public class EventAggregatorRunner {
    public static void main(String[] args) throws Exception {
        StatisticCalculator statisticCalculator = new StatisticCalculator();
        JsonPackageReader jsonPackageReader = new JsonPackageReader();
        AvroDataFileWriter avroDataFileWriter = new AvroDataFileWriter();
        statisticCalculator.calculateDate(jsonPackageReader.getEventsFromPackage(), avroDataFileWriter);
    }

}
