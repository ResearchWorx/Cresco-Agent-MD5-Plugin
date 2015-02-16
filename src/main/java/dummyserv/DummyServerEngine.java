
package dummyserv;

import plugincore.PluginEngine;




public class DummyServerEngine implements Runnable{

	
	public DummyServerEngine() 
	{
	
	}
	public void run()
	{
		System.out.println("Dummy UP...");
		while(PluginEngine.isActive)
		{
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
