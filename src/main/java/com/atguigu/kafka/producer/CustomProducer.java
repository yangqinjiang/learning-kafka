package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

/**
 * KafkaProducer：需要创建一个生产者对象，用来发送数据
 * ProducerConfig：获取所需的一系列配置参数
 * ProducerRecord：每条数据都要封装成一个 ProducerRecord 对象
 * 1.不带回调函数的API
 */
public class CustomProducer {
    public static void main(String[] args) {
        System.out.println("CustomProducer");
        Properties props = new Properties();
        //kafka集群, broker-list
        props.put("bootstrap.servers","hadoop102:9092");
        props.put("acks","all");
        //重试次数
        props.put("retries",1);
        //批次大小
        props.put("batch.size",16384);//16k
        //等待时间
        props.put("linger.ms",1);
        //RecordAccumulator缓冲区大小
        props.put("buffer.memory",33554432);//32m
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //批量发送
        for (int i = 0; i < 100; i++) {
            // topic , key ,value
            producer.send(new ProducerRecord<String, String>("first",Integer.toString(i),Integer.toString(i)));
        }
        producer.close();//注意这个
    }
}
