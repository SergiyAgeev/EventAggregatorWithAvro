package com.eventaggregator;

import com.eventaggregator.avro.model.AvroDataFileWriter;
import com.eventaggregator.avro.model.JsonPackageReader;
import com.eventaggregator.avro.model.StatisticCalculator;
import com.eventaggregator.beam.EventAggregatorOptions;
import com.eventaggregator.beam.model.BeamStatisticCalculator;
import com.eventaggregator.beam.model.EventRecord;
import com.eventaggregator.beam.model.EventStatistic;
import com.eventaggregator.beam.model.JsonParser;
import org.apache.beam.runners.dataflow.DataflowRunner;
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


public class EventAggregatorRunner {
    public static void main(String[] args) {
        EventAggregatorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(EventAggregatorOptions.class);
        options.setJobName("event-aggregator");
        options.setInputFile("gs://job_input");
        options.setOutput("gs://job_output_avro");
        options.setRunner(DataflowRunner.class);

//avro local file generate runner
//        runLocalAvro();

//beam pipeline runner
        runBeamPipeline(options);

    }

    public static void runLocalAvro() {
        StatisticCalculator statisticCalculator = new StatisticCalculator();
        AvroDataFileWriter avroDataFileWriter = new AvroDataFileWriter();
        statisticCalculator.calculateDate(JsonPackageReader.getEventsFromPackage(), avroDataFileWriter);
    }

    public static void runBeamPipeline(EventAggregatorOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Reading a Json package", TextIO.read().from(options.getInputFile()))
                .apply("Json parsing process", ParDo.of(new JsonParser()))
                .apply("Sort EventRecord by city", WithKeys.of(EventRecord::getCity))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(EventRecord.class)))
                .apply("Groping by city", GroupByKey.create())
                .apply("Calculating statistics by date", ParDo.of(new BeamStatisticCalculator()))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(EventStatistic.class)))
                .apply("Writing statistics results to avro file",
                        FileIO.<String, KV<String, EventStatistic>>writeDynamic()
                                .by(KV::getKey)
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(Contextful.fn((SerializableFunction<KV<String, EventStatistic>, EventStatistic>)
                                        KV::getValue), AvroIO.sink(EventStatistic.class))
                                .to(options.getOutput())
                                .withNaming((SerializableFunction<String, FileIO.Write.FileNaming>)
                                        EventAggregatorRunner::apply));
        pipeline
                .run()
                .waitUntilFinish();
    }

    private static FileIO.Write.FileNaming apply(String city) {
        return FileIO.Write.defaultNaming(city, ".avro");
    }

}
