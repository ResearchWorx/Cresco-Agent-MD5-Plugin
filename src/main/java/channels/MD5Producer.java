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
    	
    	
    	
    	while (PluginEngine.ProducerEnabled) 
    	{
    		try 
        	{
        		
        		if(PluginEngine.ProducerActive)
        		{
        			log();
        		}
        		
				Thread.sleep(100);
	        } 
        	catch (IOException e1) 
        	{
				e1.printStackTrace();
				System.out.println(e1);
			} 
        	catch (InterruptedException e) 
        	{
        		String msgStop = "MD5Producer Stopped";
            	PluginEngine.clog.log(msgStop);
            	
				try
		    	{
		    		if(log_channel.isOpen())
		    		{
		    			log_channel.close();
		    		}
		    	}
		    	catch(Exception ex)
		    	{
		    		String msgError = "AMPQChannel LogProducer Stopped";
		    		PluginEngine.clog.error(msgError);
		    	}		    	
			}
        	
        }
    	String msgStop = "MD5Producer Disabled";
    	PluginEngine.clog.log(msgStop);
    	
    	try 
    	{
			log(); //one last call
			
		} 
    	catch (IOException e) 
    	{
			e.printStackTrace();
		}
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
    			while (true) 
				{
    				
    				StringBuilder sb = new StringBuilder();
    				for(int i =0 ; i<PluginEngine.config.getProducerRate(); i++)
    				{
    					UUID uuid = UUID.randomUUID();
    					sb.append(uuid.toString() + ",");
    					PluginEngine.incomingCount++;
    				}
    				
    				log_channel.basicPublish(LOG_CHANNEL_NAME, "", null, sb.substring(0, sb.length() -1).toString().getBytes());
    				
    				//Thread.sleep(1000);
    				Thread.sleep(PluginEngine.config.getDataQueueDelay());
    			}
    		
    	}
    	catch(Exception ex)
    	{
    		System.out.println("MD5Producer Channel ERROR : MD5Producer : " +  ex.toString());
    	}
         			
         			
    }
    
}