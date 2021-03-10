package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 *
 * 自动提交 offset
 *
 *
 * 编写代码
 * 需要用到的类：
 * KafkaConsumer：需要创建一个消费者对象，用来消费数据
 * ConsumerConfig：获取所需的一系列配置参数
 * ConsuemrRecord：每条数据都要封装成一个 ConsumerRecord 对象
 * 为了使我们能够专注于自己的业务逻辑，Kafka 提供了自动提交 offset 的功能。
 * 自动提交 offset 的相关参数：
 * enable.auto.commit：是否开启自动提交 offset 功能
 * auto.commit.interval.ms：自动提交 offset 的时间间隔
 */
public class CustomConsumerAutoCommitOffset {
    public static void main(String[] args) {
        //自动提交 offset


        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop102:9092");
        props.put("group.id","test");
        //配置  自动提交
        props.put("enable.auto.commit","true");//开启自动提交
        props.put("auto.commit.interval.ms","1000");//自动提交的时间间隔
        //反序列化
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //订阅topic
        consumer.subscribe(Arrays.asList("first"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d , key = %s , value=%s%n",record.offset(),record.key(),record.value());
            }

        }
    }
}
