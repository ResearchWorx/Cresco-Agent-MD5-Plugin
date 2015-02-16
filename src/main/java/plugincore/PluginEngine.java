package plugincore;


import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import org.apache.commons.configuration.SubnodeConfiguration;

import channels.MD5Consumer;
import channels.MD5Producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import dummyserv.DummyServerEngine;
import shared.Clogger;
import shared.MsgEvent;
import shared.MsgEventType;
import shared.PluginImplementation;



public class PluginEngine {

	public static ConnectionFactory factory;    
    //public static Connection connection;
    //public static String LOG_CHANNEL_NAME;
    //public static Channel log_channel;
	public static long incomingCount = 0;
	public static long outgoingCount = 0;
	
	public static Thread LogConsumerThread;
	public static boolean LogConsumerActive = false;
	public static boolean LogConsumerEnabled = false;
	
	private static Thread ProducerThread;
	public static boolean ProducerActive = false;
	public static boolean ProducerEnabled = false;
	
	public static boolean isActive;
	public static PluginConfig config;
	
	public static String pluginName;
	public static String pluginVersion;
	public static String plugin;
	public static String agent;
	public static String region;
	
	public static CommandExec commandExec;
	
	public static ConcurrentMap<String,MsgEvent> rpcMap;
	public static RPCCall rpcc;
	
	public static ConcurrentLinkedQueue<MsgEvent> logOutQueue;
	
	public static WatchDog wd;
	public static WatchPerf wp;
	
	public static Clogger clog;

	public static ConcurrentLinkedQueue<MsgEvent> msgInQueue;
	
	public PluginEngine()
	{
		pluginName="MD5Plugin";
		
	}
	public void shutdown()
	{
		System.out.println("Plugin Shutdown : Agent=" + agent + "pluginname=" + plugin);
		isActive = false;
		wd.timer.cancel(); //prevent rediscovery
		wp.timer.cancel(); //prevent rediscovery
		
		try
		{
			MsgEvent me = new MsgEvent(MsgEventType.CONFIG,region,null,null,"disabled");
			me.setParam("src_region",region);
			me.setParam("src_agent",agent);
			me.setParam("src_plugin",plugin);
			me.setParam("dst_region",region);
			
			//msgOutQueue.offer(me);
			msgInQueue.offer(me);
			//PluginEngine.rpcc.call(me);
			System.out.println("Sent disable message");
		}
		catch(Exception ex)
		{
			String msg2 = "Plugin Shutdown Failed: Agent=" + agent + "pluginname=" + plugin;
			clog.error(msg2);
			
		}
		
	}
	public String getName()
	{
		   return pluginName; 
	}
	public String getVersion() //This should pull the version information from jar Meta data
    {
		   String version;
		   try{
		   String jarFile = PluginImplementation.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		   File file = new File(jarFile.substring(5, (jarFile.length() -2)));
           FileInputStream fis = new FileInputStream(file);
           @SuppressWarnings("resource")
		   JarInputStream jarStream = new JarInputStream(fis);
		   Manifest mf = jarStream.getManifest();
		   
		   Attributes mainAttribs = mf.getMainAttributes();
           version = mainAttribs.getValue("Implementation-Version");
		   }
		   catch(Exception ex)
		   {
			   String msg = "Unable to determine Plugin Version " + ex.toString();
			   clog.error(msg);
			   version = "Unable to determine Version";
		   }
		   
		   return pluginName + "." + version;
	   }
	//steps to init the plugin
	public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue,ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region,String agent, String plugin)  
	{
		isActive = true;
		commandExec = new CommandExec();
		rpcMap = new ConcurrentHashMap<String,MsgEvent>();
		rpcc = new RPCCall();
		
		//this.msgOutQueue = msgOutQueue; //send directly to log queue
		this.msgInQueue = msgInQueue; //messages to agent should go here
		
		this.agent = agent;
		this.plugin = plugin;
		
		this.region = region;
		try{
			
			if(msgInQueue == null)
			{
				System.out.println("MsgInQueue==null");
				return false;
			}
			
			this.config = new PluginConfig(configObj);
			
			//create logger
			clog = new Clogger(msgInQueue,region,agent,plugin); //send logs directly to outqueue
			
			String startmsg = "Initializing Plugin: Region=" + region + " Agent=" + agent + " plugin=" + plugin + " version" + getVersion();
			clog.log(startmsg);
			
			
			try{
	    		//establish AMPQ connectivity
	    		factory = new ConnectionFactory();
	    		factory.setHost(PluginEngine.config.getAMPQControlHost());
	    		factory.setUsername(PluginEngine.config.getAMPQControlUser());
	    		factory.setPassword(PluginEngine.config.getAMPQControlPassword());
	    		factory.setConnectionTimeout(10000);
	    		
	    	}
	    	catch(Exception ex)
	    	{
	    		System.out.println("MD5 AMPQ Plugin Init error: " + ex.toString());
	    		return false;
	    	}
			
			MD5Producer v = new MD5Producer();
	    	ProducerThread = new Thread(v);
	    	if(config.getLogProducerEnabled())
	    	{
	    		ProducerThread.start();
	    		while(!ProducerEnabled)
	    		{
	    			Thread.sleep(1000);
	    			String msg = "Waiting for MD5Producer Initialization : Region=" + region + " Agent=" + agent + " plugin=" + plugin;
	    			clog.log(msg);
	    		}
	    	}
	    	PluginEngine.ProducerActive = true;
	    	
	    	MD5Consumer lc = new MD5Consumer();
    		LogConsumerThread = new Thread(lc);
	    	if(config.getLogConsumerEnabled())
	    	{
	    		LogConsumerThread.start();
	    		while(!LogConsumerEnabled)
		    	{
		    		Thread.sleep(1000);
		    		String msg = "Waiting for MD5Consumer Initialization : Region=" + region + " Agent=" + agent + " plugin=" + plugin;
		    		clog.log(msg);
		    	}
	    		PluginEngine.LogConsumerActive = true;
		    	
		    	
	    	}
			/*
	    	try
	    	{
	    		System.out.println("Starting MD5 Service");
				DummyServerEngine dummyEngine = new DummyServerEngine();
				Thread dummyServerThread = new Thread(dummyEngine);
		        dummyServerThread.start();
		        
	    	}
	    	catch(Exception ex)
	    	{
	    		System.out.println("Unable to Start HTTP Service : " + ex.toString());
	    	}
	    	*/
	    	
	    	
	    	/*
	    	AMPQLogProducer v = new AMPQLogProducer();
	    	ProducerThread = new Thread(v);
	    	ProducerThread.start();
	    	while(!ProducerEnabled)
	    	{
	    		Thread.sleep(1000);
	    		String msg = "Waiting for AMPQProducer Initialization : Region=" + region + " Agent=" + agent + " plugin=" + plugin;
	    		clog.log(msg);
	    	}
	    	*/
	    	
	    	
	    	wd = new WatchDog();
			wp = new WatchPerf();
	    	
    		return true;
    		
		
		}
		catch(Exception ex)
		{
			String msg = "ERROR IN PLUGIN: : Region=" + region + " Agent=" + agent + " plugin=" + plugin + " " + ex.toString();
			clog.error(msg);
			return false;
		}
		
	}
	
	public void msgIn(MsgEvent me)
	{
		
		final MsgEvent ce = me;
		try
		{
		Thread thread = new Thread(){
		    public void run(){
		
		    	try 
		        {
					MsgEvent re = commandExec.cmdExec(ce);
					if(re != null)
					{
						re.setReturn(); //reverse to-from for return
						msgInQueue.offer(re); //send message back to queue
					}
					
				} 
		        catch(Exception ex)
		        {
		        	System.out.println("Controller : PluginEngine : msgIn Thread: " + ex.toString());
		        }
		    }
		  };
		  thread.start();
		}
		catch(Exception ex)
		{
			System.out.println("Controller : PluginEngine : msgIn Thread: " + ex.toString());        	
		}
		
	}
		
		
}
