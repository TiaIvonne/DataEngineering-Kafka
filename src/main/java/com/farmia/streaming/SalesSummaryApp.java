package com.farmia.streaming;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
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
                    (key, record) ->record.get("category").toString(),
                        Grouped.with(Serdes.String(), genericAvroSerde))
                // Configura el procesamiento en intervalos de 1 minuto
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
                )
                // Criterios de agregacion para obtener el totalRevenue
                .aggregate(
                        // Cuando llega la primera transaccion este es el valor de partida
                        () -> "0,0.0",
                        (category, record, accumulator) -> {
                            String[] parts = accumulator.split(",");
                            int totalQuantity = Integer.parseInt(parts[0]) + (int) record.get("quantity");
                            // Este bloque convierte el decimal de MySql a formato double
                            Conversions.DecimalConversion decimalConversion = new org.apache.avro.Conversions.DecimalConversion();
                            org.apache.avro.Schema priceSchema = record.getSchema().getField("price").schema();
                            double price = decimalConversion.fromBytes(
                                    (java.nio.ByteBuffer) record.get("price"),
                                    priceSchema,
                                    priceSchema.getLogicalType()
                            ).doubleValue();

                            double totalRevenue = Double.parseDouble(parts[1]) + price * (int) record.get("quantity");
                            return totalQuantity + "," + totalRevenue;
                        },
                        Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .map(
                        (windowedKey, value) -> {
                            String[] parts = value.split(",");
                            int totalQuantity = Integer.parseInt(parts[0]);
                            double totalRevenue = Double.parseDouble(parts[1]);
                            String json = String.format(
                                    Locale.US,
                                    "{\"category\":\"%s\",\"total_quantity\":%d,\"total_revenue\":%.2f,\"window_start\":%d,\"window_end\":%d}",
                                    windowedKey.key(),
                                    totalQuantity,
                                    totalRevenue,
                                    windowedKey.window().start(),
                                    windowedKey.window().end()
                            );
                            return KeyValue.pair(windowedKey.key(), json);
        })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sales-summary-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        Topology topology = createTopology();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Thread.currentThread().join();
    }
}
