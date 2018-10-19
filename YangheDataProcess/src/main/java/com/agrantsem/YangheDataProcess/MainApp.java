package com.agrantsem.YangheDataProcess;

import java.util.Timer;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agrantsem.YangheDataProcess.util.HttpServerUtil;


/**
 * Hello world!
 *
 */
public class MainApp 
{
	private static Logger LOGGER = LoggerFactory.getLogger("tracking");
    public static void main( String[] args )
    {
    	PropertyConfigurator.configure("config/log4j.properties");
    	
		YangheDataTask task = new YangheDataTask();
		if(args!=null && args.length==2 && args[0].equals("sendemail")){
			task.dayWork(args[1], true);
		}else if(args==null || args.length==0){
			Timer timer = new Timer();
			timer.schedule(task, 0, 600000);
			
			Thread server=new HttpServerUtil();
			server.run();
		}
    }
}
