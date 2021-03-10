package com.atguigu.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 时间戳拦截器
 * 实现一个简单的双 interceptor 组成的拦截链。第一个 interceptor 会在消息发送前将时间
 * 戳信息加到消息 value 的最前部；第二个 interceptor 会在消息发送后更新成功发送消息数或
 * 失败发送消息数。
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //创建一个新的record, 把时间戳写入消息体的最前部
        //注意, 不要在拦截器内修改topic,partition值,
        //用户可以在该方法中对消息做任何操作，但最好
        //保证不要修改消息所属的 topic 和分区，否则会影响目标分区的计算。
        return new ProducerRecord<String, String>(record.topic(),record.partition(),record.timestamp(),record.key(),System.currentTimeMillis() + ","+record.value());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
