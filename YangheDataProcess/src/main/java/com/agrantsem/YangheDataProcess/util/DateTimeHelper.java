/**
 * 
 */
package com.agrantsem.YangheDataProcess.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/**
 * @author yubin.wang
 *
 */
public class DateTimeHelper {

	private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	public static final Long MILLIS_SECONDS_ONE_DAY =  86400000L; //一天的毫秒数
	
	/**
	 * @return 昨日日期 格式如 20160218
	 */
	public static String getYesterdayDate(String format){
		String result=null;
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1);
		Date date = calendar.getTime();
		if(format==null){
			result = dateFormat.format(date);
		}else{
			DateFormat df = new SimpleDateFormat(format);
			result = df.format(date);
		}
		return result;	
	}
	
	/**
	 * @return 昨日日期 格式如 20160218
	 */
	public static String getDateString(String format,Date dt){
		String result=null;

		if(format==null){
			result = dateFormat.format(dt);
		}else{
			DateFormat df = new SimpleDateFormat(format);
			result = df.format(dt);
		}
		return result;	
	}
	
	public static Date getYesterdayDate(){
		Date now = new Date();
		Calendar cal = Calendar.getInstance();
		cal.setTime(now);
		cal.add(Calendar.DATE, -1);
		Date ystd = cal.getTime();
		return ystd;
	}
	public static Long getTodayByLong() {
		Calendar calendar = Calendar.getInstance();
		Long time = calendar.getTimeInMillis();
		return time;
	}
	
	public static Long getYesterdayDateByDate() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1);
		Long time = calendar.getTimeInMillis();
		return time;
	}
	
	/**
	 * @return 当前日期 格式如 20160219 
	 */
	public static String getCurrentDate(){
		Calendar calendar = Calendar.getInstance();
		Date date = calendar.getTime();
		String result = dateFormat.format(date);
		return result;
	}
	
	/**
	 * 获取当前小时数
	 * @return
	 */
	public static int getHourNumber() {
		Calendar calendar = Calendar.getInstance();
		return calendar.get(Calendar.HOUR_OF_DAY);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String day1 = getCurrentDate();
		String day2 = getCurrentDate();
		System.out.println(day1.equals(day2));
	}

}
