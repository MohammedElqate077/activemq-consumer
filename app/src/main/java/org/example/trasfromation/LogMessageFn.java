package org.example.trasfromation;

import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;

public class LogMessageFn extends DoFn<JmsRecord, Void> {
    private final Logger logger;

    // Constructor that takes the main class logger
    public LogMessageFn(Logger logger) {
        this.logger = logger;
    }

    @ProcessElement
    public void processElement(@Element JmsRecord jmsRecord) {
        try {
            // Get the payload as string
            String text = jmsRecord.getPayload();
            logger.info("Received message: {}", text);

            // If you need to access the raw message
            String messageText = jmsRecord.getPayload();
            logger.info("Message text from raw message: {}", messageText);
        } catch (Exception e) {
            logger.error("Error processing message: {}", e.getMessage(), e);
        }
    }
}