package com.agrantsem.YangheDataProcess.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpServerUtil extends Thread {

	private static Logger LOGGER = LoggerFactory.getLogger("tracking");
	private static String PORT = PropsUtil.getConfig().getProperty("httpserver_port");
	

	@Override
	public void run() {
		try {
			int port=Integer.parseInt(PORT);
            ServerSocket ss=new ServerSocket(port);

            while(true){
                Socket socket=ss.accept();
                BufferedReader bd=new BufferedReader(new InputStreamReader(socket.getInputStream()));
                
                /**
                 * 接受HTTP请求
                 */
                String requestHeader;
                boolean ret=false;
                while((requestHeader=bd.readLine())!=null&&!requestHeader.isEmpty()){
                    /**
                     * 获得GET参数
                     */
                    if(requestHeader.startsWith("GET")){
                        int begin = requestHeader.indexOf("upfile=")+7;
                        int end = requestHeader.indexOf("HTTP/");
                        String condition=requestHeader.substring(begin, end);
                        LOGGER.debug("http GET ："+condition);
                        ret=FtpUtil.upFile(condition.trim());
                    }
                }
               
                //发送回执
                PrintWriter pw=new PrintWriter(socket.getOutputStream());
                
                pw.println("HTTP/1.1 200 OK");
                pw.println("Content-type:text/html");
                pw.println();
                if(ret){
                	pw.println("<h1>success</h1>");
                }else{
                	pw.println("<h1>upfile fail!!!</h1>");
                }
                
                pw.flush();
                socket.close();
            }
        } catch (Exception e) {
        	LOGGER.error("",e);
        }
	}

	public static void main(String[] args) {
		Thread server=new HttpServerUtil();
		server.run();
	}
}
