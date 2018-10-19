package com.agrantsem.YangheDataProcess;

import com.agrantsem.YangheDataProcess.util.DateTimeHelper;
import com.agrantsem.YangheDataProcess.util.MailSender;
import com.agrantsem.YangheDataProcess.util.PropsUtil;
import com.agrantsem.YangheDataProcess.util.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.TimerTask;


public class YangheDataTask extends TimerTask {
    private static Logger LOGGER = LoggerFactory.getLogger("tracking");
    public static final int DEFAULT_TIMEOUT = 5 * 60 * 1000;
    //"C:/Users/think/Desktop/yanghe/data/";
    private static String YangPath = PropsUtil.getConfig().getProperty("yanghe_local_path");
    String prex = "ang-";
    private static String DoneName = PropsUtil.getConfig().getProperty("successName");
    private static String ShowName = PropsUtil.getConfig().getProperty("ShowName");
    private static String comm_getDonePrex = PropsUtil.getConfig().getProperty("comm_getDonePrex") + " ";
    private static String comm_getShowPrex = PropsUtil.getConfig().getProperty("comm_getShowPrex") + " ";//加空格

    private static String comm_getTTReqPrex = PropsUtil.getConfig().getProperty("comm_getTTReqPrex") + " ";
    private static String comm_getTTVrdPrex = PropsUtil.getConfig().getProperty("comm_getTTVrdPrex") + " ";
    private static String TTReqName = PropsUtil.getConfig().getProperty("TTReqName");
    private static String TTVrdName = PropsUtil.getConfig().getProperty("TTVrdName");

    //添加百分点反馈的vrd数据
    private static String BFDVrdName = PropsUtil.getConfig().getProperty("BFDVrdName");
    private static String comm_bfdbrdPrex = PropsUtil.getConfig().getProperty("comm_bfdbrdPrex") + " ";

    private static String YangheMagicPathName = YangPath + "magic.txt";
    private static String messageForShowProcess = "";
    //http
    private static String httpServer = PropsUtil.getConfig().getProperty("httpserve");
    private static String httpPort = PropsUtil.getConfig().getProperty("httpserver_port", "9876");

    private static String mail_targets = PropsUtil.getConfig().getProperty("mail_targets");
    int HOUR_TO_RUN = Integer.parseInt(PropsUtil.getConfig().getProperty("HOUR_TO_RUN", "8"));

    @Override
    public void run() {
        int currentHourNumber = DateTimeHelper.getHourNumber();
        // 判断今日是否已经执行了，lastime记录上次发送的天
        if (currentHourNumber >= HOUR_TO_RUN) {
            String yesterdayDate = DateTimeHelper.getYesterdayDate(null);//是yymmdd
            dayWork(yesterdayDate, true);
        } else {
            LOGGER.debug("Please wait to proccess yanghe Report.");
        }
    }

    public void dayWork(String dateFormate, boolean sendEmail) {
        LOGGER.debug("enter dayWork");

        String showOrginPathName = (YangPath + ShowName).replaceAll("yymmdd", dateFormate);

        String showRetPathName = (YangPath + prex + ShowName).replaceAll("yymmdd", dateFormate);
        String showDone = showRetPathName + ".done";

        String ttReqOrginPathName = (YangPath + TTReqName).replaceAll("yymmdd", dateFormate);
        String ttReqRetPathName = (YangPath + prex + TTReqName).replaceAll("yymmdd", dateFormate);
        String ttReqDone = ttReqRetPathName + ".done";

        if (TxtFileUtil.isExists(showDone) && TxtFileUtil.isExists(ttReqDone)) {
            LOGGER.debug("today had done berfor,no need tod");
            LOGGER.debug("out dayWork");
            return;
        }
        //hdfs文件生成了
        if (isHdfsDone(dateFormate)) {
            //用shell从hdfs上获取show，tt
            downShowTTResultFromHdfs(dateFormate, showOrginPathName, ttReqOrginPathName);
            if (TxtFileUtil.isExists(showOrginPathName) && TxtFileUtil.isExists(ttReqOrginPathName)) {
                boolean ret = dealShowTT(showOrginPathName, showRetPathName,
                        ttReqOrginPathName, ttReqRetPathName, showDone, ttReqDone);
                //boolean ret2 = dealShowTT(null,null,ttVrdOrginPathName,ttVrdRetPathName,showDone,ttVrdDone);
                if (ret && sendEmail) {
                    MailSender sender = new MailSender();
                    //String messag=messageForShowProcess+"<br>如果数据没问题，请点击此链接上传ftp:<a href=\"http://"+httpServer+":"+httpPort+"/"+"upfile="+dateFormate+"\">确认上传</a>";
                    String messag = messageForShowProcess + "<br>如果数据没问题，请点击此链接上传";
                    ret = sender.sendHTMLOut(mail_targets,
                            "洋河数据" + dateFormate, messag,
                            new String[]{showRetPathName, ttReqRetPathName});
                    if (ret == true) {
                        LOGGER.debug("send report success");
                    } else {
                        LOGGER.error("send report fail!!!");
                    }
                }
            } else {
                LOGGER.warn(showOrginPathName + " or " + ttReqOrginPathName + "or"
                        + "not exit !!!，wait next time to process！！！");
            }

        } else {
            LOGGER.warn("done file not exit !!!，wait next time to process！！！");
        }
        LOGGER.debug("out dayWork");
    }

    private boolean isHdfsDone(String currentDate) {
        String donelocalPath = (YangPath + DoneName).replaceAll("yymmdd", currentDate);
        if (TxtFileUtil.isExists(donelocalPath)) {
            return true;
        }
        //还没有，那先从hdfs下载
        String comm_getDone = (comm_getDonePrex + donelocalPath).replaceAll("yymmdd", currentDate);
        System.out.println(comm_getDone);
        Worker.exec(comm_getDone, 5 * 60 * 1000);

        return TxtFileUtil.isExists(donelocalPath);
    }

    private void downShowTTResultFromHdfs(String currentDate, String showOrginPathName, String ttReqOrginPathName) {
        if (!TxtFileUtil.isExists(showOrginPathName)) {
            String comm_getShow = comm_getShowPrex + showOrginPathName;
            comm_getShow = comm_getShow.replaceAll("yymmdd", currentDate);
            Worker.exec(comm_getShow, 5 * 60 * 1000);
        }
        if (!TxtFileUtil.isExists(ttReqOrginPathName)) {
            String comm_getTTReq = comm_getTTReqPrex + ttReqOrginPathName;
            comm_getTTReq = comm_getTTReq.replaceAll("yymmdd", currentDate);
            Worker.exec(comm_getTTReq, 5 * 60 * 1000);
        }
    }

    public boolean dealShowTT(String showOrginPathName, String showRetPathName,
                              String ttOrginPathName, String ttRetPathName, String showDone,
                              String ttDone) {
        boolean retResult = true;
        if (!TxtFileUtil.isExists(showDone)) {
            //先删除数据，防止前面遗留的错误文件或者没有执行完的文件
            TxtFileUtil.deleteFile(showRetPathName);
            int ret = TxtFileUtil.processShowCmFile(showOrginPathName, showRetPathName);
            if (ret >= 0) {
                if (ret > 0) {
                    messageForShowProcess = "对于超过五次展示的show 少补了" + ret + "条记录";
                } else {
                    messageForShowProcess = "对于超过五次展示的show 补全了";
                }
                TxtFileUtil.saveDone(showDone);
            } else {
                retResult = false;
                LOGGER.error(showOrginPathName + ",TxtFileUtil.processShowCmFile return -1!!!");
            }
        }
        if (!TxtFileUtil.isExists(ttDone)) {
            TxtFileUtil.deleteFile(ttRetPathName);
            HashSet<String> yangheset = TxtFileUtil.getYangheSetFromFile(YangheMagicPathName);
            boolean ret = TxtFileUtil.processTTFile(ttOrginPathName, ttRetPathName, yangheset, YangheMagicPathName);
            if (ret) {
                TxtFileUtil.saveDone(ttDone);
            } else {
                retResult = false;
                LOGGER.error(ttOrginPathName + ",TxtFileUtil.processTTFile return false!!!");
            }
        }

        return retResult;
    }


    //这个提供给命令行 ，线上用，请不要随意改
    public static void main(String[] args) {
        YangheDataTask task = new YangheDataTask();
        if (args != null && args.length == 2) {
            //两个参数，show和tt目录
            //task.dealShowTT(args[0], args[0] + ".out", args[1], args[1] + ".out", args[0] + ".done", args[1] + ".out");
        } else if (args != null && args.length == 1) {
            //一个参数，时间 如 20170220
            String currentDate = args[0];
            task.dayWork(currentDate, false);
        } else if (args == null || args.length == 0) {
            //task.dealShowTT("C:/Users/think/Desktop/yanghe/data/yanghe-show-20170219","C:/Users/think/Desktop/yanghe/data/ang-yanghe-show-20170219","C:/Users/think/Desktop/yanghe/data/yanghe-tt-20170219","C:/Users/think/Desktop/yanghe/data/ang-yanghe-tt-20170219","C:/Users/think/Desktop/yanghe/data/yanghe-show-20170219.done","C:/Users/think/Desktop/yanghe/data/yanghe-tt-20170219.done");
            task.dayWork("20170331", false);
        }
    }
}