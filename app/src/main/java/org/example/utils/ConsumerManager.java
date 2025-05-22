package org.example.utils;

import java.io.Closeable;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ConsumerManager implements Runnable, Closeable {
   private final String BROKER_URL;
   private final String USERNAME;
   private final String PASSWORD;
   private String queueName;
   private String messageType;
   private ActiveMQConnectionFactory connectionFactory;
   private Connection connection;
   private Session session;
   private MessageConsumer consumer;
   private boolean isRunning = false;
   private ObjectMapper mapper = new ObjectMapper();

   public ConsumerManager(String brokerUrl, String username, String password) {
      this.BROKER_URL = brokerUrl;
      this.USERNAME = username;
      this.PASSWORD = password;
   }

   public Connection getConnection() throws JMSException {
      System.out.println("Connecting to ActiveMQ broker at " + this.BROKER_URL);
      this.connectionFactory = new ActiveMQConnectionFactory(this.BROKER_URL);
      if (this.USERNAME != null && !this.USERNAME.isEmpty()) {
         this.connectionFactory.setUserName(this.USERNAME);
         this.connectionFactory.setPassword(this.PASSWORD);
      }

      this.connection = this.connectionFactory.createConnection();
      return this.connection;
   }

   public void connect() throws JMSException {
      if (this.connection == null) {
         this.getConnection();
      }

      this.connection.start();
      this.session = this.connection.createSession(false, 1);
      System.out.println("Connected to ActiveMQ broker successfully");
   }

   public void setupConsumer(String queueName, String messageType) throws Exception {
      this.queueName = queueName;
      this.messageType = messageType;
      if (this.session == null) {
         this.connect();
      }

      Destination destination = this.session.createQueue(queueName);
      this.consumer = this.session.createConsumer(destination);
      this.mapper = new ObjectMapper();
   }

   public void startListening() {
      try {
         isRunning = true;

         // Set up a message listener
         consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
               try {
                  processMessage(message);
               } catch (Exception e) {
                  System.err.println("Error processing message: " + e.getMessage());
                  e.printStackTrace();
               }
            }
         });
      } catch (JMSException e) {
         System.err.println("Error setting up message listener: " + e.getMessage());
         e.printStackTrace();
      }
   }

   public void consumeMessages() {
      this.isRunning = true;

      try {
         while (this.isRunning) {
            Message message = this.consumer.receive(1000);
            if (message != null) {
               this.processMessage(message);
            }
         }
      } catch (JMSException e) {
         System.err.println("Error consuming messages: " + e.getMessage());
         e.printStackTrace();
      }

   }

   private static void processMessage(Message message) throws JMSException {
      if (message instanceof TextMessage textMessage) {
         System.out.println("Received text message: " + textMessage.getText());
         System.out.println("Message ID: " + message.getJMSMessageID());
         System.out.println("Correlation ID: " + message.getJMSCorrelationID());
      } else if (message instanceof BytesMessage bytesMessage) {
         byte[] byteData = new byte[(int) bytesMessage.getBodyLength()];
         bytesMessage.readBytes(byteData);
         System.out.println("Received bytes message of length: " + byteData.length);
      } else if (message instanceof MapMessage mapMessage) {
         System.out.println("Received map message");
      } else if (message instanceof ObjectMessage objectMessage) {
         System.out.println("Received object message: " + String.valueOf(objectMessage.getObject()));
      } else {
         System.out.println("Received message of type: " + message.getClass().getName());
      }

   }

   public void stop() {
      this.isRunning = false;
      this.close();
   }

   public void run() {
      System.out.println("Listening for messages on queue: " + this.queueName);

      try {
         this.startListening();
         System.out.println("Consumer is now running. Press Ctrl+C to exit.");
         Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            this.stop();
         }));
         Thread.currentThread().join();
      } catch (InterruptedException e) {
         System.err.println("Consumer interrupted: " + e.getMessage());
      }

   }

   public void close() {
      try {
         if (this.consumer != null) {
            this.consumer.close();
         }

         if (this.session != null) {
            this.session.close();
         }

         if (this.connection != null) {
            this.connection.close();
         }

         System.out.println("Disconnected from ActiveMQ broker");
      } catch (JMSException e) {
         System.err.println("Error closing resources: " + e.getMessage());
         e.printStackTrace();
      }

   }
}
