package com.agrantsem.YangheDataProcess.util;

import com.agrantsem.YangheDataProcess.TxtFileUtil;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class FtpUtil {
	private static Logger logger = LoggerFactory.getLogger("tracking");
	
	private static String ftpServer=PropsUtil.getConfig().getProperty("ftpServer");
	private static int ftpPort=Integer.parseInt(PropsUtil.getConfig().getProperty("ftpPort","9876"));
	private static String ftpName=PropsUtil.getConfig().getProperty("ftpName");
	private static String ftpPwd=PropsUtil.getConfig().getProperty("ftpPwd");
	private static String ftpPath=PropsUtil.getConfig().getProperty("ftpPath");
	
	private static String prex="ang-";
	private static String YangPath=PropsUtil.getConfig().getProperty("yanghe_local_path");
	private static String ShowName=PropsUtil.getConfig().getProperty("ShowName");
	private static String TTName=PropsUtil.getConfig().getProperty("TTName");
    private static String ftpVrdPath=PropsUtil.getConfig().getProperty("ftpVrdPath");
	
	
	/** 
     * Description: 向FTP服务器上传文件 
     * @param url FTP服务器hostname 
     * @param port FTP服务器端口，如果默认端口请写-1 
     * @param username FTP登录账号 
     * @param password FTP登录密码 
     * @param path FTP服务器保存目录 
     * @param filename 上传到FTP服务器上的文件名 
     * @param input 输入流 
     * @return 成功返回true，否则返回false 
     */  
	
    public static boolean uploadFile(String url, int port, String username, String password, String path,  
        String filename, InputStream input)  
    {  
        boolean success = false;  
        FTPClient ftp = new FTPClient();  
        try  
        {  
            int reply;  
              
            // 连接FTP服务器  
            if (port > -1)  
            {  
                ftp.connect(url, port);  
            }  
            else  
            {  
                ftp.connect(url);  
            }  
              
            // 登录FTP  
            success=ftp.login(username, password);
            if(success==false){
            	logger.error("ftp.login(username, password) login err!!,return false"); 
            	return success;
            }
            reply = ftp.getReplyCode();  
            if (!FTPReply.isPositiveCompletion(reply))  
            {  
                ftp.disconnect();  
                return success;  
            }  
            success=ftp.changeWorkingDirectory(path);  
            ftp.enterLocalPassiveMode();
            success=ftp.storeFile(filename, input);  
            if(success==false){
            	logger.error("ftp.storeFile(filename, input),return false"); 
            	return success;
            }  
            //input.close();  
            ftp.logout();  
            success = true;  
        }  
        catch (IOException e)  
        {  
            success = false;  
            logger.error("",e);  
        }  
        finally  
        {  
            if (ftp.isConnected())  
            {  
                try  
                {  
                    ftp.disconnect();  
                }  
                catch (IOException e)  
                {  
                    logger.error("", e);  
                }  
            }  
        }  
        return success;  
    }

	public static boolean upFile(String date) {
		boolean ret=false;
		try {
			String showRetPathName=(YangPath+prex+ShowName).replaceAll("yymmdd", date);
			String ttRetPathName=(YangPath+prex+TTName).replaceAll("yymmdd", date);
			
			ret = upFileByPath(showRetPathName) && upFileByPath(ttRetPathName);
		} catch (Exception e) {
			if(ret){
				logger.error("upfile "+date+" get Exception !!,but upfile success,maybe just file close err",e);
			}else{
				logger.error("upfile "+date+" get Exception !!,and upfile fail!!!",e);
			}
		}
		
		return  ret;
	}

	private static boolean upFileByPath(String path)
			throws FileNotFoundException, IOException {
		boolean ret;	
		int beginIndex = path.lastIndexOf("/")+1;
		String fileName=path.substring(beginIndex,path.length());
		logger.debug("upfile:["+path+"]"+",ftp filename:["+fileName+"]");
		FileInputStream in = new FileInputStream(new File(path));
		ret = uploadFile(ftpServer, ftpPort,ftpName, ftpPwd, ftpPath, fileName, in);
		//ret = uploadFile("l-hgw1.prod.ud2.corp.agrant.cn", 3722,"ftp_user", "WzWXzPwRn8nfUJ", "/", "test.txt", in);
		in.close();
		if(ret==true){
			logger.debug("upfile "+path+" sucess "); 
		}else{
			logger.error("upfile "+path+" fail!!! "); 
		}
		return ret;
	}

    /**
     * Description: 从FTP服务器下载文件
     * @Version1.0 Jul 27, 2008 5:32:36 PM by 崔红保（cuihongbao@d-heaven.com）创建
     * @param url FTP服务器hostname
     * @param port FTP服务器端口
     * @param username FTP登录账号
     * @param password FTP登录密码
     * @param remotePath FTP服务器上的相对路径
     * @param fileName 要下载的文件名
     * @param localPath 下载后保存到本地的路径
     * @return
     */
    public static boolean downFile(String url, int port,String username, String password, String remotePath,String fileName,String localPath) {
        boolean success = false;
        FTPClient ftp = new FTPClient();
        try {
            int reply;
            ftp.connect(url, port);
            //如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器
            ftp.login(username, password);//登录
            reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
                return success;
            }
            ftp.changeWorkingDirectory(remotePath);//转移到FTP服务器目录
            ftp.enterLocalPassiveMode();
            FTPFile[] fs = ftp.listFiles();
            for(FTPFile ff:fs){
                if(ff.getName().equals(fileName)){
                    File localFile = new File(localPath+"/"+ff.getName());

                    OutputStream is = new FileOutputStream(localFile);
                    ftp.retrieveFile(ff.getName(), is);
                    is.close();
                    TxtFileUtil.saveDone(localPath+"/"+ff.getName()+".done");
                }
            }

            ftp.logout();
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (ftp.isConnected()) {
                try {
                    ftp.disconnect();
                } catch (IOException ioe) {
                }
            }
        }
        return success;
    }

    public static boolean downVrdFile(String filename,String localPath){
        if(filename.length()>0&&localPath.length()>0){
            logger.debug("begin tp get bfdvrd");
            return downFile(ftpServer,ftpPort,ftpName,ftpPwd,ftpVrdPath,filename,localPath);
        }

        logger.debug("[ftp|downVrdFile]filename or localPath is wrong");
        return false;
    }

    public static void main(String[] args) {
        //upFile(args[0]);
        String filename="ang-yanghe-vrd-20170331";
        String localpath="E:\\codeTemp\\";
        downVrdFile(filename,localpath);
    }

}
