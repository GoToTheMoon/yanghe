package com.agrantsem.YangheDataProcess.util;

import org.apache.log4j.Logger;

public class Worker extends Thread {
	private static Logger logger = Logger.getLogger("tracking");
	
	public final Process process;
	public Integer exit = -1;

	public Worker(Process process) {
		this.process = process;
	}

	public void run() {
		try {
			exit = process.waitFor();
		} catch (Exception ignore) {
			logger.error(ignore);
		}
	}
	
	public static void exec(String command, int timeout){
		logger.info("Start to execute command: " + command);
		Process process = null;
        try
        {
        	process = Runtime.getRuntime().exec(command);
            Worker worker = new Worker(process);
            worker.start();
            try {
                worker.join(timeout);
                if (worker.exit != null){
                	logger.info("command success command="+command);
                } else{
                	logger.info("command timetoue command="+command);
                }
            } catch (Exception ex) {
            	try{
            		worker.interrupt();
            	}catch(Exception e){
            		
            	}
                logger.info("command exception command="+command);
            } finally {
                process.destroy();
            }
            
        } catch ( Exception ex ){
           logger.error(ex);
        }
        logger.info("Finished to execute command: " + command);
	}

}
