package com.barry.kafkawordcount.processors;

import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ButtonCountProcessor {

    @Bean
    public Consumer<KStream<String, String>> process(){

        return input ->
            input.foreach((key, value)->{
                System.out.println("Key:" + key + " Value:"  + value);
            });
    }

}