package org.example.trasfromation;

import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;

public class ProceesMessages extends DoFn<JmsRecord, Void> {
    private final Logger logger;

    public ProceesMessages(Logger logger) {
        this.logger = logger;
    }

    @ProcessElement
    public void processMessage(@Element JmsRecord jmsRecord) throws Exception {
        try {
            System.out.println("Received text message: " + jmsRecord.getPayload());
            System.out.println("Message ID: " + jmsRecord.getJmsMessageID());
            System.out.println("Correlation ID: " + jmsRecord.getJmsCorrelationID());
            // System.out.println("Propery");
            // jmsRecord.getProperties().forEach((key, value) -> System.out.print(key + ":" + value + " "));
        } catch (Exception e) {
            logger.error("Error processing message: {}", e.getMessage(), e);
        }

    }

}