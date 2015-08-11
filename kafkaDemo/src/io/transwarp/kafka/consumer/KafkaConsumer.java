package io.transwarp.kafka.consumer;


import io.transwarp.kafka.producer.KafkaProducer;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class KafkaConsumer extends Thread {
	private static Log LOG = LogFactory.getLog(KafkaConsumer.class);
	private final ConsumerConnector consumer;
	private final String topic;
	public static Properties props=new Properties();
	
	static{
		URL conf = Thread.currentThread().getContextClassLoader()
				.getResource("consumer.properties");
		try {
			props.load(conf.openStream());
		} catch (IOException e) {
			LOG.error(e.getMessage(), e.getCause());
		}
	}

	public KafkaConsumer() {
		
		
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));
		this.topic = props.getProperty("topic");
	}

	

	@Override
	public void run() {
		Map<String, Integer> topickMap = new HashMap<String, Integer>();
		int partitions=Integer.parseInt(KafkaProducer.props.getProperty("partitions.number"));
		topickMap.put(topic, partitions);
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer
				.createMessageStreams(topickMap);
		
		ExecutorService executor=Executors.newFixedThreadPool(partitions);
		
		List<KafkaStream<byte[], byte[]>> kss=streamMap.get(topic);
		for (Iterator<KafkaStream<byte[], byte[]>> it = kss.iterator(); it.hasNext();) {
			final KafkaStream<byte[], byte[]> kafkaStream = (KafkaStream<byte[], byte[]>) it
					.next();
			executor.submit(new Runnable() {
				
				@Override
				public void run() {
					
					ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
					while (it.hasNext()) {
						MessageAndMetadata<byte[], byte[]> message=it.next();
						LOG.info("key: "+new String(message.key())+"   value:" + new String(message.message()));
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							LOG.info(e.getMessage(),e.fillInStackTrace());
						}
					}
				}
			});
			
		}
		
		
	}
	public static void main(String[] args) {
		KafkaConsumer kc=new KafkaConsumer();
		kc.start();
	}
}
