package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 手动提交 offset
 * 虽然自动提交 offset 十分简介便利，但由于其是基于时间提交的，开发人员难以把握
 * offset 提交的时机。因此 Kafka 还提供了手动提交 offset 的 API。
 * 手动提交 offset 的方法有两种：分别是 commitSync（同步提交）和 commitAsync（异步
 * 提交）。两者的相同点是，都会将本次 poll 的一批数据最高的偏移量提交；不同点是，
 * commitSync 阻塞当前线程，一直到提交成功，并且会自动失败重试（由不可控因素导致，
 * 也会出现提交失败）；而 commitAsync 则没有失败重试机制，故有可能提交失败
 */
public class CustomComsumerCommitSync {
    public static void main(String[] args) {
        //由于同步提交 offset 有失败重试机制，故更加可靠


        Properties props = new Properties();
        props.put("bootstrap.servers","hadoop102:9092");
        //消费者组, 只要group.id相同,就属于同一个消费者组
        props.put("group.id","test");
        //注意,关闭自动提交offset
        props.put("enable.auto.commit","false");//开启自动提交
//        props.put("auto.commit.interval.ms","1000");//自动提交的时间间隔
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
            //同步提交,当前线程会阻塞,直到offset提交成功
//            System.out.println("commitSync~");
            consumer.commitSync();

        }
    }
}
