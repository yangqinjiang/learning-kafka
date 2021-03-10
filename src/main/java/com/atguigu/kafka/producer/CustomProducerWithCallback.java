package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 带回调函数的 API
 * 回调函数会在 producer 收到 ack 时调用，为异步调用，该方法有两个参数，分别是
 * RecordMetadata 和 Exception，如果 Exception 为 null，说明消息发送成功，如果
 * Exception 不为 null，说明消息发送失败。
 * 注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试。
 */
public class CustomProducerWithCallback {
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
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), "with callback func: "+Integer.toString(i)),
                    new Callback() {
                //回调函数 , 该方法会在Producer收到ack时调用, 为异步调用
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            //如果 Exception 为 null，说明消息发送成功，如果
                            //Exception 不为 null，说明消息发送失败
                            if(e == null){
                                System.out.println("success->"+recordMetadata.offset());
                            }else{
                                e.printStackTrace();
                            }
                        }
                    });
        }
        producer.close();//注意这个
    }
}
