package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
//统计发送消息成功和发送失败消息数，并在 producer 关闭时打印这两个计数器
public class CounterInterceptor implements ProducerInterceptor<String,String> {
    //在此拦截器,定义两个属性,记录发送消息成功和发送失败消息数
    private int errorCounter = 0;
    private int successCounter = 0;
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord; //直接返回, 不作修改
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        //该方法会在消息从 RecordAccumulator 成功发送到 Kafka Broker 之后，或者在发送过程
        //中失败时调用
        // 统计成功和失败的次数
        if (e == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }

    public void close() {
//        关闭 interceptor，主要用于执行一些资源清理工作
//        在 producer 关闭时打印这两个计数器
        // 保存结果
        System.out.println("Successful sent: " + successCounter);
        System.out.println("Failed sent: " + errorCounter);
    }

    public void configure(Map<String, ?> map) {

    }
}
