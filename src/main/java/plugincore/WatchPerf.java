package plugincore;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import shared.MsgEvent;
import shared.MsgEventType;



public class WatchPerf {
	public Timer timer;
	private long startTS;
	private Map<String,String> wdMap;
	private Random rand;
	
	private long lastIncoming;
	private long lastOutgoing;
	private long lastRunTime;
	
	
	public WatchPerf() {
		
		  lastIncoming = 0;
	 	  lastOutgoing = 0;
		  lastRunTime = 0;
		
		  rand = new Random();
		  startTS = System.currentTimeMillis();
		  timer = new Timer();
	      timer.scheduleAtFixedRate(new WatchDogTask(), 1000, PluginEngine.config.getWatchDogTimer());
	      wdMap = new HashMap<String,String>(); //for sending future WD messages
	      	  
	      MsgEvent le = new MsgEvent(MsgEventType.INFO,PluginEngine.config.getRegion(),null,null,"WatchDog timer set to " + PluginEngine.config.getWatchDogTimer() + " milliseconds");
	      le.setParam("src_region", PluginEngine.region);
		  le.setParam("src_agent", PluginEngine.agent);
		  le.setParam("src_plugin", PluginEngine.plugin);
		  le.setParam("dst_region", PluginEngine.region);
		  PluginEngine.clog.log(le);
	      
	  }


	class WatchDogTask extends TimerTask {
		
	    public void run() {
	    	
	    	long runTime = System.currentTimeMillis() - startTS;
	    	 MsgEvent le = new MsgEvent(MsgEventType.WATCHDOG,PluginEngine.region,null,null,"WatchDog timer set to " + PluginEngine.config.getWatchDogTimer() + " milliseconds");
	    	 le.setParam("src_region", PluginEngine.region);
			 le.setParam("src_agent", PluginEngine.agent);
			 le.setParam("src_plugin", PluginEngine.plugin);
			 le.setParam("dst_region", PluginEngine.region);
			 le.setParam("isGlobal", "true");
			 le.setParam("application", PluginEngine.config.getPerfAppName());
			 
			 le.setParam("incomingCount", String.valueOf(PluginEngine.incomingCount));
			if((PluginEngine.incomingCount == 0))
			 {
				 le.setParam("perfin","0.0");
			 }
			 else
			 {
				 double num = PluginEngine.incomingCount - lastIncoming;
				 double denom = (runTime - lastRunTime)/1000;
				 double perfin = num/denom;
				 lastIncoming = PluginEngine.incomingCount;
				 lastRunTime = runTime;
				 le.setParam("perfin",String.valueOf(perfin));
				 
			 }
			 le.setParam("outgoingCount", String.valueOf(PluginEngine.outgoingCount));
				
			if((PluginEngine.outgoingCount == 0))
			 {
				 le.setParam("perfout","0.0");
			 }
			 else
			 {
				 double num = PluginEngine.outgoingCount - lastOutgoing;
				 double denom = (runTime - lastRunTime)/1000;
				 double perfout = num/denom;
				 lastIncoming = PluginEngine.outgoingCount;
				 lastRunTime = runTime;
				 le.setParam("perfout",String.valueOf(perfout));
			 } 
			 
			 //le.setParam("perfmetric",String.valueOf(rand.nextInt(100 - 0 + 1) + 0));
			 le.setParam("runtime", String.valueOf(runTime));
			 le.setParam("timestamp", String.valueOf(System.currentTimeMillis()));
			 //System.out.println(le.getParamsString());
			 PluginEngine.clog.log(le);
	    }
	  }

}
