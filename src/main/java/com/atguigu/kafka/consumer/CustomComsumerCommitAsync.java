package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * ）异步提交 offset
 * 虽然同步提交 offset 更可靠一些，但是由于其会阻塞当前线程，直到提交成功。因此吞
 * 吐量会收到很大的影响。因此更多的情况下，会选用异步提交 offset 的方式。
 */
public class CustomComsumerCommitAsync {
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
            //异步提交,当前线程不阻塞
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                    if (e != null) {
                        System.err.println("Commit failed for" +  offsets);
                    }
                }
            });

        }
    }
}
