package io.transwarp.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KafkaProducer extends Thread{

	private static Log LOG = LogFactory.getLog(KafkaProducer.class);
	private final Producer<String, byte[]> producer;
	private final String topic;
	public static Properties props=new Properties();
	
	static{
		URL conf = Thread.currentThread().getContextClassLoader()
				.getResource("producer.properties");
		try {
			props.load(conf.openStream());
		} catch (IOException e) {
			LOG.error(e.getMessage(), e.getCause());
		}
	}

	public KafkaProducer() {
		URL conf = Thread.currentThread().getContextClassLoader()
				.getResource("producer.properties");
		try {
			props.load(conf.openStream());
		} catch (IOException e) {
			LOG.error(e.getMessage(), e.getCause());
		}
		
		producer = new kafka.javaapi.producer.Producer<String, byte[]>(
				new ProducerConfig(props));
		this.topic = props.getProperty("topic");
	}

	@Override
	public void run() {
		try {
			int i=1;
			while (true) {
				
				LOG.info(i);
				
				producer.send(new KeyedMessage<String, byte[]>(this.topic, i+"", "test".getBytes()));
				i++;
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e.fillInStackTrace());
		}
	}
	public static void main(String[] args) {
		KafkaProducer producer=new KafkaProducer();
		producer.start(); 
		
	}

}
