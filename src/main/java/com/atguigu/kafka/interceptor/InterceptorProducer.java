package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) throws InterruptedException {
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

        //构建拦截器
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.atguigu.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.kafka.interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //批量发送
        for (int i = 0; i < 100; i++) {
            Thread.sleep(50);
            // topic , key ,value
            producer.send(new ProducerRecord<String, String>("first",Integer.toString(i),"message: "+Integer.toString(i)));
        }
        producer.close();//注意这个
    }
}
