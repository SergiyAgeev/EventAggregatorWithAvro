package com.eventaggregator;

import com.eventaggregator.avro.model.AvroDataFileWriter;
import com.eventaggregator.avro.model.JsonPackageReader;
import com.eventaggregator.avro.model.StatisticCalculator;
import com.eventaggregator.beam.EventAggregatorOptions;
import com.eventaggregator.beam.model.BeamStatisticCalculator;
import com.eventaggregator.beam.model.EventRecord;
import com.eventaggregator.beam.model.EventStatistic;
import com.eventaggregator.beam.model.JsonParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventAggregatorRunner {
    private static final Logger LOG = LoggerFactory.getLogger(BeamStatisticCalculator.class);

    public static void main(String[] args) {
//avro local file generate runner
//        runLocalAvro();

        EventAggregatorOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(EventAggregatorOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Reading a Json package", TextIO.read().from(options.getInputFile()))
                .apply("Json parsing process", ParDo.of(new JsonParser()))
                .apply("Sort EventRecord by city", WithKeys.of(EventRecord::getCity))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(EventRecord.class)))
                .apply("Groping by city", GroupByKey.create())
                .apply("Calculating statistics by date", ParDo.of(new BeamStatisticCalculator()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(EventStatistic.class)))
                .apply("Writing statistics results to avro file", FileIO.<String, KV<String, EventStatistic>>writeDynamic()
                        .by(KV::getKey)
                        .withDestinationCoder(StringUtf8Coder.of())
                        .via(Contextful.fn((SerializableFunction<KV<String, EventStatistic>, EventStatistic>) KV::getValue),
                                AvroIO.sink(EventStatistic.class))
                        .to(options.getOutput())
                        .withNaming((SerializableFunction<String, FileIO.Write.FileNaming>) city ->
                                FileIO.Write.defaultNaming(city, ".avro")));

        pipeline.run().waitUntilFinish();

    }

    public static void runLocalAvro() {
        StatisticCalculator statisticCalculator = new StatisticCalculator();
//        JsonPackageReader jsonPackageReader = new JsonPackageReader();
        AvroDataFileWriter avroDataFileWriter = new AvroDataFileWriter();
        statisticCalculator.calculateDate(JsonPackageReader.getEventsFromPackage(), avroDataFileWriter);
    }

}
