package io.transwarp.kafka.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;  
import java.util.Collection;  
import java.util.List;  
import java.util.Properties;  
  
import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
public class LogProducer {  
  
    private Producer<String,String> inner;  
    public LogProducer() throws Exception{  
        Properties properties = new Properties();  
        properties.load(ClassLoader.getSystemResourceAsStream("producer.properties"));  
        properties.put("partitioner.class", "io.transwarp.kafka.consumer.ProducerPartitioner");
        ProducerConfig config = new ProducerConfig(properties);  
       
        inner = new Producer<String, String>(config);  
    }  
  
      
    public void send(String topicName,String message) {  
        if(topicName == null || message == null){  
            return;  
        }  
        String key=message.split(",")[18];
        KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName,key,message);//如果具有多个partitions,请使用new KeyedMessage(String topicName,K key,V value).  
        inner.send(km);  
    }  
      
    public void send(String topicName,Collection<String> messages) {  
        if(topicName == null || messages == null){  
            return;  
        }  
        if(messages.isEmpty()){  
            return;  
        }  
        List<KeyedMessage<String, String>> kms = new ArrayList<KeyedMessage<String, String>>();  
        for(String entry : messages){  
        	String key=entry.split(",")[18];
            KeyedMessage<String, String> km = new KeyedMessage<String, String>(topicName,key,entry);  
            kms.add(km);  
        }  
        inner.send(kms);  
    }  
      
    public void close(){  
        inner.close();  
    }  
      
    /** 
     * @param args 
     */  
    public static void main(String[] args) {  
        LogProducer producer = null;  
        try{  
            producer = new LogProducer();  
            int i=0;  
            while(true){  
            	File file=new File("D:\\eclipse\\workspace5\\stream\\data\\test_01_000001");
//            	 File file=new File("/root/kafkaProducer/data/test_01_000001");
            	BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            	
            	String line;
            	while((line=br.readLine())!=null){
            		producer.send("test-topic", line);
            		i++;
            		 Thread.currentThread().sleep(1000);  
            	}
            	br.close();
            	System.out.println("count : "+i);
                Thread.sleep(10000);  
            }  
        }catch(Exception e){  
            e.printStackTrace();  
        }finally{  
            if(producer != null){  
                producer.close();  
            }  
        }  
  
    }  
  
}