package com.eventaggregator.beam.model;

import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonParser extends DoFn<String, EventRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
    @ProcessElement
    public void processElement(@Element String stringData, OutputReceiver<EventRecord> outputReceiver) {
        LOG.info("parse process for json: " + stringData);
        EventRecord eventRecord = new Gson().fromJson(stringData, EventRecord.class);
        LOG.info("json parsed in event: " + eventRecord);
        outputReceiver.output(eventRecord);
    }

}
