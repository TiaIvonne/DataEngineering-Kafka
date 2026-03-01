package com.farmia.streaming;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SensorAlerterApp {

    private static final String INPUT_TOPIC = "sensor-telemetry";
    private static final String OUTPUT_TOPIC = "sensor-alerts";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static Topology createTopology() {
        final Map<String, String> serdeConfig = Collections.singletonMap(
                "schema.registry.url", SCHEMA_REGISTRY_URL
        );

        final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> sensorStream = builder.stream(
                INPUT_TOPIC, Consumed.with(Serdes.String(), genericAvroSerde)
        );

        sensorStream
                .filter((key, record) -> {
                    float temperature = (float) record.get("temperature");
                    float humidity = (float) record.get("humidity");
                    return temperature > 35.0f || humidity < 20.0f;
                })
                .mapValues(record -> {
                    String sensorId = record.get("sensor_id").toString();
                    long timestamp = (long) record.get("timestamp");
                    float temperature = (float) record.get("temperature");
                    float humidity = (float) record.get("humidity");

                    String alertType;
                    String details;
                    if (temperature > 35.0f) {
                        alertType = "HIGH_TEMPERATURE";
                        details = "Temperature exceeded 35C: " + temperature;
                    } else {
                        alertType = "LOW_HUMIDITY";
                        details = "Humidity below 20%: " + humidity;
                    }

                    return String.format(
                            "{\"sensor_id\":\"%s\",\"alert_type\":\"%s\",\"timestamp\":%d,\"details\":\"%s\"}",
                            sensorId, alertType, timestamp, details
                    );
                })
                .peek((key, value) -> System.out.println("Alert generated - key: " + key + " value: " + value))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sensor-alerter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        Topology topology = createTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
