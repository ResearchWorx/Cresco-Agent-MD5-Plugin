package channels;
import java.io.IOException;
import java.util.UUID;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import plugincore.PluginEngine;


public class MD5Producer implements Runnable {

	private Connection connection;
    private Channel log_channel;
    private String LOG_CHANNEL_NAME;
    private String payload;
    
    public MD5Producer() 
    {
    
    }
    
    public void run() {
        
    	try
    	{
    		Connection connection = PluginEngine.factory.newConnection();
    		LOG_CHANNEL_NAME = PluginEngine.config.getDataQueueName();
    		log_channel = connection.createChannel();
    		log_channel.exchangeDeclare(LOG_CHANNEL_NAME, "fanout");
    		
    		StringBuilder sb = new StringBuilder();
			for(int i =0 ; i<PluginEngine.config.getProducerRate(); i++)
			{
				UUID uuid = UUID.randomUUID();
				sb.append(uuid.toString() + "\n");
				//PluginEngine.incomingCount++;
			}
    		payload = sb.substring(0, sb.length() -1).toString();
    		
    	}
    	catch(Exception ex)
    	{
    		System.err.println("MD5Producer Initialization Failed:  Exiting");
    		System.err.println(ex);
    		return; //don't kill app if log can't be established
    	}
    
    	PluginEngine.ProducerEnabled = true;
    	
    	String msg = "MD5Producer Started";
    	PluginEngine.clog.log(msg);
    	
    	try{
    	 //while(!Thread.currentThread().isInterrupted()) 
    	while (PluginEngine.ProducerEnabled) 
    	{
    		try 
        	{
        		
        		if(PluginEngine.ProducerActive)
        		{
        			log();
        			//System.out.println("Log: ProducerEnabled: " + PluginEngine.ProducerEnabled + " ProducerActive:" + PluginEngine.ProducerActive);
        		}
        		
				//Thread.sleep(100);
				Thread.sleep(PluginEngine.config.getDataQueueDelay());
				
	        } 
        	catch (Exception ex) 
        	{
        		String msgStop = "MD5Producer Error: " + ex.toString();
            	PluginEngine.clog.log(msgStop);
            	
			} 
        	
        }
    	}
    	catch (Exception ex) 
    	{
    		PluginEngine.ProducerActive = false;
    		String msgStop = "MD5Producer Interupt Error: " + ex.toString();
        	PluginEngine.clog.log(msgStop);
        	
		} 
    	
    	System.out.println("Plugin Shutdown : Agent=" + PluginEngine.agent + "pluginname=" + PluginEngine.plugin + " closing producer log");
		
    	try
    	{
    		if(log_channel.isOpen())
    		{
    			log_channel.close();
    		}
    		
    	}
    	catch(Exception ex)
    	{
    		String msgError = "MS5Producer Error close log_channel: " + ex.toString();
    		PluginEngine.clog.error(msgError);
    	}
    	
    	try
    	{
    		if(connection.isOpen())
    		{
    			connection.close();
    		}
    	}
    	catch(Exception ex)
    	{
    		String msgError = "MS5Producer Error close connection: " + ex.toString();
    		PluginEngine.clog.error(msgError);
    	}
    	
    	PluginEngine.ProducerActive = false;
    	
    	String msgStop = "MD5Producer Disabled";
    	PluginEngine.clog.log(msgStop);
    	//System.out.println(msgStop)
    	
    	return;
    }

    private void log() throws IOException 
    {
    	
    	try
    	{
    		if(!log_channel.isOpen())
    		{
    			System.out.println("Reconnecting Channel");
    			log_channel = connection.createChannel();
    			log_channel.exchangeDeclare(LOG_CHANNEL_NAME, "fanout");
    		}
    	}
    	catch(Exception ex)
    	{
    		System.out.println(ex);
    	}
    	try
    	{
    		long begin = System.nanoTime();
    		StringBuilder sb = new StringBuilder();
			for(int i =0 ; i<PluginEngine.config.getProducerRate(); i++)
			{
				UUID uuid = UUID.randomUUID();
				sb.append(uuid.toString() + "\n");
				//PluginEngine.incomingCount++;
			}
	
    		//log_channel.basicPublish(LOG_CHANNEL_NAME, "", null, payload.getBytes());
			log_channel.basicPublish(LOG_CHANNEL_NAME, "", null, sb.substring(0, sb.length() -1).toString().getBytes());
  			
			long end = System.nanoTime();
  	  		long dt = end - begin;
  	  	    PluginEngine.etIn = dt;
	  		
    		/*
    		long begin = System.currentTimeMillis();
    		
    				StringBuilder sb = new StringBuilder();
    				for(int i =0 ; i<PluginEngine.config.getProducerRate(); i++)
    				{
    					UUID uuid = UUID.randomUUID();
    					sb.append(uuid.toString() + "\n");
    					//PluginEngine.incomingCount++;
    				}
    				//System.out.println("*IN" + sb.substring(0, sb.length() -1).toString() + "*IN");
        			
    				log_channel.basicPublish(LOG_CHANNEL_NAME, "", null, sb.substring(0, sb.length() -1).toString().getBytes());
  			long end = System.currentTimeMillis();
  	  		long dt = end - begin;
  	  		PluginEngine.etIn = dt;
  	  		sb.setLength(0);
  	  		
    				//Thread.sleep(1000);
    				//Thread.sleep(PluginEngine.config.getDataQueueDelay());
    		 */
    	}
    	catch(Exception ex)
    	{
    		System.out.println("MD5Producer Channel ERROR : MD5Producer : " +  ex.toString());
    	}
         			
         			
    }
    
}