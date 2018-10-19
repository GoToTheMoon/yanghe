/**
 * 
 */
package com.agrantsem.YangheDataProcess.util;

import java.io.*;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yubin.wang
 * 读取配置文件
 */
public final class PropsUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger("tracking");
	private static Properties CONFIG;
	
	static{
		setConfig(PropsUtil.loadProps("config.properties"));
	}
	
	/**
	 * 加载config文件
	 * 
	 */
	public static Properties loadProps(String filname) {
		Properties props = null;
		//FileReader fr = null;
		BufferedReader fr=null;
		try {
			// 在src/main/resources下寻找该文件
			InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filname);
			// 寻找config/config.properties文件
			//fr = new FileReader(new File(filname));
			fr=new BufferedReader(new InputStreamReader(is));
			props = new Properties();
			props.load(fr);
		} catch (IOException e) {
			LOGGER.error("load properties file failure", e);
		} finally {
			if (fr != null) {
				try {
					fr.close();
				} catch (IOException e) {
					LOGGER.error("close input stream failure", e);
				}
			}
		}
		return props;
	}
	
	/**
	 * 获取字符型属性（默认值为空字符串） 
	 */
	public static String getString(Properties props, String key) {
		return getString(props, key, "");
	}
	
	/**
	 * 获取字符型属性（可指定默认值）
	 */
	public static String getString(Properties props, String key, String defaultValue) {
		String value = defaultValue;
		if (props.containsKey(key)) {
			value =props.getProperty(key);
		}
		return value;
	}
	
	/**
	 * 使用分隔符;或,分隔字符串
	 */
	public static String[] getSpilitStrings(String str) {
		return str.split(";|,");
	}
	
	public static void main(String[] args) {
		Properties props = PropsUtil.loadProps("config.properties");
		String YangPath = PropsUtil.getConfig().getProperty("yanghe_local_path");
		System.out.println(YangPath);
		String str = PropsUtil.getString(props, "bidder.urls");
		String[] array = PropsUtil.getSpilitStrings(str);
		for (String s : array) {
			System.out.println(s);
		}
		System.out.println(PropsUtil.getString(props, "jdbc.driver"));
	}

	public static Properties getConfig() {
		return CONFIG;
	}

	public static void setConfig(Properties cONFIG) {
		CONFIG = cONFIG;
	}
}
