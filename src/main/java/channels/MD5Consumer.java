package channels;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import plugincore.PluginEngine;
import shared.MsgEvent;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;


public class MD5Consumer implements Runnable {

	private QueueingConsumer consumer;
 	//private Unmarshaller LogEventUnmarshaller;
	private Connection connection;
    private Channel channel;
    private String LOG_CHANNEL_NAME;

 	public MD5Consumer() throws JAXBException
	{
		//JAXBContext jaxbContext = JAXBContext.newInstance(MsgEvent.class);
	    //LogEventUnmarshaller = jaxbContext.createUnmarshaller();    
	}
	
	
	public void run() {
        
    	try
    	{
    		
    		//Channel channel = PluginEngine.connection.createChannel();
    		//channel.exchangeDeclare(PluginEngine.LOG_CHANNEL_NAME, "fanout");
    		//String queueName = channel.queueDeclare().getQueue();
    		//channel.queueBind(queueName, PluginEngine.LOG_CHANNEL_NAME, "");
    		Connection connection = PluginEngine.factory.newConnection();
    		LOG_CHANNEL_NAME = PluginEngine.config.getDataQueueName();
    		channel = connection.createChannel();
    		channel.exchangeDeclare(LOG_CHANNEL_NAME, "fanout");
    		String queueName = channel.queueDeclare().getQueue();
    		channel.queueBind(queueName, LOG_CHANNEL_NAME, "");
       		consumer = new QueueingConsumer(channel);
    		channel.basicConsume(queueName, true, consumer); 
    		
    		PluginEngine.LogConsumerEnabled = true;
    	}
    	catch(Exception ex)
    	{
    		System.err.println("LogConsumer Initialization Failed:  Exiting");
    		System.err.println(ex);
    		return;
    	}
    	
    	//while (PluginEngine.LogConsumerEnabled)
    	try {
    		while (PluginEngine.LogConsumerEnabled)
    		{  
    	    try 
        	{
        		if(PluginEngine.LogConsumerActive)
        		{
        			//consumer.getChannel().close();
        			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        			String message = new String(delivery.getBody());
        			ArrayList<String> uuidList = new ArrayList<String>();
        			System.out.println("*OUT" + message + "*OUT");
        			if(message.contains("\n"))
        			{
        				String[] sstr = message.split("\n");
        				for(String str : sstr)
        				{
        					uuidList.add(str);
        				}
        			}
        			else
        			{
        				uuidList.add(message);
        			}

        			for(String uuid : uuidList)
        			{
        				PluginEngine.outgoingCount++;
        				MessageDigest m = MessageDigest.getInstance("MD5");
            	        m.update(uuid.getBytes(),0,uuid.length());
            	        //System.out.println("MD5: "+new BigInteger(1,m.digest()).toString(16));			
        			}
        			
        		}
        		
				//Thread.sleep(100);
				Thread.sleep(PluginEngine.config.getDataQueueDelay());
        	}
        	catch (Exception ex)
        	{
        		System.out.println("ERROR : Log Consumer : " + ex.toString());
        	}
    	      }
    	   } 
    	catch (Exception ex)
    	{
    		System.out.println("ERROR : Log Consumer : " + ex.toString());
    		PluginEngine.LogConsumerActive = false;
    	}
    	
    	PluginEngine.LogConsumerActive = false;
    return;	
    }
	
}
