package org.example;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.example.trasfromation.LogMessageFn;
import org.example.trasfromation.ProceesMessages;
import org.example.utils.Config;
// import org.example.utils.ConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public interface Options extends PipelineOptions, GcpOptions {

        @Description("Path to Consumer Config YAML file")
        @Default.String("/home/mohammed/Projects/IdeaProjects/activemq_apachebeam/consumer/config/config.yaml")
        String getConsumerConfigFilePath();

        void setConsumerConfigFilePath(String value);

        @Description("BigQuery project ID")
        @Default.String("your-project-id")
        String getBqProjectId();

        void setBqProjectId(String value);

        @Description("BigQuery dataset name")
        @Default.String("your_dataset")
        String getBqDataset();

        void setBqDataset(String value);

        @Description("BigQuery table name")
        @Default.String("your_table")
        String getBqTable();

        void setBqTable(String value);

        @Description("Path to service account JSON key file")
        @Default.String("service-account-key.json")
        String getBqServiceAccountKeyPath();

        void setBqServiceAccountKeyPath(String value);

        @Description("Batch size for BigQuery insertions")
        @Default.Integer(500)
        Integer getBatchSize();

        void setBatchSize(Integer value);
    }

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(Options.class);

        // logger.info("Input File: {}", options.getInputFile());
        logger.info("BigQuery Service Account Path: {}", options.getBqServiceAccountKeyPath());
        logger.info("BigQuery Destination: {}:{}:{}",
                options.getBqProjectId(),
                options.getBqDataset(),
                options.getBqTable());

        // directConsumer(options.getConsumerConfigFilePath());
        Pipeline pipeline = Pipeline.create(options);

        Config config = Config.readConfigFromYaml(options.getConsumerConfigFilePath());

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(config.broker.host);

        pipeline.apply("Read Messages from ActiveMQ",
                JmsIO.read()
                        .withConnectionFactory(connectionFactory)
                        .withQueue(config.producerSetting.get(0).queue_name)
                        .withUsername(config.broker.username)
                        .withPassword(config.broker.password)
            )
            // .apply("Log Messages", ParDo.of(new LogMessageFn(logger)));
            .apply("Log Messages", ParDo.of(new ProceesMessages(logger)));
            
        logger.info("Starting Apache Beam pipeline to consume from queue");
        
        pipeline.run();
    }
    
}