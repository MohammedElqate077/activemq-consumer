package org.example.utils;

import java.io.File;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class Config {
    public Broker broker;
    public List<ProducerSetting> producerSetting;
    public Scheduling scheduling;

    public static class Broker {
        public String host;
        public String username;
        public String password;

        @Override
        public String toString() {
            return "Broker{" +
                    "host='" + host + '\'' +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    '}';
        }
    }

    public static class ProducerSetting {
        public String queue_name;
        public String messageType;
        public Long batchMessage;

        @Override
        public String toString() {
            return "ProducerSetting{" +
                    "queue_name='" + queue_name + '\'' +
                    ", messageType='" + messageType + '\'' +
                    ", batchMessage=" + batchMessage +
                    '}';
        }
    }

    public static class Scheduling {
        public int period;
        public String timeUnit;

        @Override
        public String toString() {
            return "Scheduling{" +
                    "period=" + period +
                    ", timeUnit=" + timeUnit +
                    '}';
        }
    }


    public static Config readConfigFromYaml(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(filePath), Config.class);
    }

    @Override
    public String toString() {
        return "Config{" +
                "broker=" + broker +
                ", producerSetting=" + producerSetting +
                ", scheduling=" + scheduling +
                '}';
    }
}
