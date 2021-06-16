package com.eventaggregator.beam;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface EventAggregatorOptions extends PipelineOptions {
    @Description("Path of the package to read from")
    @Default.String("src/main/java/com/eventaggregator/in")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the package to write to")
    @Default.String("src/main/java/com/eventaggregator/out")
    @Validation.Required
    String getOutput();

    void setOutput(String value);

}
