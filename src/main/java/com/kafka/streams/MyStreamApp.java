package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class MyStreamApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String>  wordCountInput =  builder.stream("word-count-input");

        KTable<String, Long> wordCounts =  wordCountInput.mapValues((ValueMapper<String, String>) String::toLowerCase)
                                                        .flatMapValues(value -> Arrays.asList(value.split(" ")))
                                                        .selectKey((ignoredKey, word) -> word)
                                                        .groupByKey()
                                                        .count(Materialized.as("count"));


        wordCounts.toStream().to("word-count-output", Produced.with(stringSerde, longSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
            streams.start();

            //Print topology
            System.out.println(streams.toString());

            //Add shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            streams.close();
        }));
    }
}
