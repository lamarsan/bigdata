package com.spark.kafka;

import kafka.Kafka;

/**
 * className: KafkaClientApp
 * description: TODO
 *
 * @author hasee
 * @version 1.0
 * @date 2019/1/22 16:14
 */
public class KafkaClientApp {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }
}
