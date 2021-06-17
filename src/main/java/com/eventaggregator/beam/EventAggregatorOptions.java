package com.eventaggregator.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface EventAggregatorOptions extends PipelineOptions {
    @Description("Path of the package to read from")
//    @Default.String("src/main/java/com/eventaggregator/in/*.json")
    @Default.String("gs://job_input")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the package to write to")
//    @Default.String("src/main/java/com/eventaggregator/out")
    @Default.String("gs://job_output_avro")
    String getOutput();

    void setOutput(String value);

}
