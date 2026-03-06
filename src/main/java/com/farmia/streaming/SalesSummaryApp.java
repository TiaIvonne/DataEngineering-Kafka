package com.farmia.streaming;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SalesSummaryApp {

    private static final String INPUT_TOPIC = "sales-transactions";
    private static final String OUTPUT_TOPIC = "sales-summary";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static Topology createTopology() {
        final Map<String, String> serdesConfig = Collections.singletonMap(
                "schema.registry.url", SCHEMA_REGISTRY_URL
        );
        final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(serdesConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> salesStream = builder.stream(
                INPUT_TOPIC, Consumed.with(Serdes.String(), genericAvroSerde)
        );
        salesStream
                // Configuracion de los requerimientos de agrupacion de los datos de categoria x minuto
                .groupBy(
                    (key, record) ->record.get("category").toString(), Grouped.with(Serdes.String(), genericAvroSerde))
                // Configura el procesamiento en intervalos de 1 minuto
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .aggregate(
                        () -> "0,0.0",
                        (category,record, accumulator) -> {
                            String[] parts = accumulator.split(",");
                            int totalQuantity = Integer.parseInt(parts[0]) + (int) record.get("quantity");
                            double totalRevenue = Double.parseDouble(parts[1])
                                    + ((Number) record.get("price")).doubleValue() * ((Number) record.get("quantity")).intValue();
                            return totalQuantity + "," + totalRevenue;
                        },
                        Materialized.with(Serdes.String(), Serdes.String()))
                .toStream();

        
        return builder.build();

    }

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        Topology topology = createTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
