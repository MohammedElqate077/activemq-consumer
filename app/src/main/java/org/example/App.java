package org.example;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.example.utils.Config;
import org.example.utils.ConsumerManager;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public interface Options extends PipelineOptions, GcpOptions {
        @Description("GCS input file pattern")
        @Default.String("gs://your-bucket/your-file.csv")
        String getInputFile();

        void setInputFile(String value);

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

        logger.info("Input File: {}", options.getInputFile());
        logger.info("BigQuery Service Account Path: {}", options.getBqServiceAccountKeyPath());
        logger.info("BigQuery Destination: {}:{}:{}",
                options.getBqProjectId(),
                options.getBqDataset(),
                options.getBqTable());

        // TODO: Enable this when ready
        DirectConsumer(options.getConsumerConfigFilePath());
    }

    public static void DirectConsumer(String configPath) throws Exception {
        Config config = Config.readConfigFromYaml(configPath);

        for (Config.ProducerSetting producerSetting : config.producerSetting) {
            ConsumerManager consumer = new ConsumerManager(
                    config.broker.host,
                    config.broker.username,
                    config.broker.password);

            consumer.connect();
            consumer.setupConsumer(producerSetting.queue_name, producerSetting.messageType);

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(
                    consumer,
                    0L,
                    config.scheduling.period,
                    TimeUnit.valueOf(config.scheduling.timeUnit));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    scheduler.shutdown();
                    consumer.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
        }
    }

}