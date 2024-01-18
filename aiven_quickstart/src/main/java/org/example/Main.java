package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka-321b7939-t0895299-9a1d.a.aivencloud.com:26589");
        properties.put("security.protocol", "SSL");
        properties.put("ssl.keystore.type", "PKCS12");
        properties.put("ssl.keystore.location", "/samples/aiven_certs/ssl/client.keystore.p12");
        properties.put("ssl.keystore.password", "demopass");
        // only if different from keystore password: properties.put("ssl.key.password", "{KEY_PASSWORD}");
        properties.put("ssl.truststore.type", "JKS");
        properties.put("ssl.truststore.location", "/samples/aiven_certs/ssl/client.truststore.jks");
        properties.put("ssl.truststore.password", "demotrust");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        Random r = new Random();
        int low=5;
        int high=120;
        int randomTemp;


        for (int i = 0; i < 6; i++) {
            // single record creation
            String key = UUID.randomUUID().toString();

            TimeZone tz = TimeZone.getTimeZone("UTC");
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
            df.setTimeZone(tz);
            String nowAsISO = df.format(new Date());

            randomTemp = r.nextInt(high - low) + low; // get an int between high and low inclusive


            JSONObject jsonObject = new JSONObject();
            jsonObject.put("scale", "F");
            jsonObject.put("temperature", randomTemp);
            jsonObject.put("timestamp", nowAsISO);
            jsonObject.put("sequence", i);

            try {
                Thread.sleep(450); // sleep a bit so the timestamps aren't all the same
            } catch (InterruptedException e) {
                // expected exception so ignore
            }

            // send the single record
            producer.send(new ProducerRecord<String, String>("demotopic", key, jsonObject.toString()));
        }

        producer.close();
    }



}