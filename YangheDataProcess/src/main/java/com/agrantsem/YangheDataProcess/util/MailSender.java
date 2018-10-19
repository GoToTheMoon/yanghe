package com.agrantsem.YangheDataProcess.util;

import java.io.File;
import java.util.*;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.SendFailedException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MailSender {
	
	private String smtp_server = null;
	private String smtp_server_port = null;
	private String smtp_user = null;
	private String smtp_password = null;
	private static final Logger LogHelper = LoggerFactory.getLogger("tracking");
	public MailSender() {
		smtp_server = PropsUtil.getConfig().getProperty("smtp_mail_server");
		smtp_server_port = PropsUtil.getConfig().getProperty("smtp_mail_server_port");
		smtp_user =  PropsUtil.getConfig().getProperty("smtp_mail_username");
		smtp_password =  PropsUtil.getConfig().getProperty("smtp_mail_password");
	}

	public boolean sendOut(String mail_targets, String title, String content, String filePath) {
		// result
		boolean ret = false;
		
		// Get the session object
		Properties props = new Properties();
		props.put("mail.smtp.host", smtp_server);
		props.put("mail.smtp.port", smtp_server_port);
		props.put("mail.smtp.socketFactory.port", smtp_server_port);
		props.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.connectiontimeout", "120000");
		props.put("mail.smtp.timeout", "120000");

		Session session = Session.getDefaultInstance(props,
				new javax.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(smtp_user, smtp_password);
					}
				});

		LogHelper.info("About to send mail, title=" + title + ",attachment=" + filePath);
		
		try {
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);

			// Set From: header field of the header.
			message.setFrom(new InternetAddress(smtp_user));

			// Set To: header field of the header.
			ArrayList<String> targets = new ArrayList<String>();
			if (mail_targets.contains(";")){
				String[] ts = mail_targets.split(";");
				targets.addAll(Arrays.asList(ts));
			}else{
				targets.add(mail_targets);
			}
			for(String target :targets){
				message.addRecipient(Message.RecipientType.TO, new InternetAddress(target));
			}

			// Set Subject: header field
			message.setSubject(title);
			
			// Create a multipar message
			Multipart multipart = new MimeMultipart();

			// Create the message part
			BodyPart messageContentPart = new MimeBodyPart();
			messageContentPart.setText(content);
						
			// Set text message part
			multipart.addBodyPart(messageContentPart);

			if (filePath != null){
				File file = new File(filePath);
				if (file.exists()){
					// Part two is attachment
					MimeBodyPart messageattachmentPart = new MimeBodyPart();
					String filename = file.getAbsolutePath();
					DataSource source = new FileDataSource(filename);
					messageattachmentPart.setDataHandler(new DataHandler(source));
					messageattachmentPart.setFileName(file.getName());
					multipart.addBodyPart(messageattachmentPart);
				}
			}

			// Send the complete message parts
			message.setContent(multipart);

			// Send message
			Transport.send(message);
			
			LogHelper.info("Sent out mail, title=" + title);
			
			ret = true;
			
		} catch (MessagingException mex) {
			LogHelper.error("",mex);
			LogHelper.info("Error for send out mail, title=" + title);
		}catch(Exception ex){
			LogHelper.error("",ex);
			LogHelper.info("Error for send out mail, title=" + title);
		}
		return ret;
	}
	
	public boolean sendHTMLOut(String mail_targets, String title, String content, String[] filePath){
		StringBuffer invalids = new StringBuffer();
		boolean ret = sendHTMLOut(mail_targets, title, content, filePath, invalids);
		if (!ret && invalids.length() > 0){
			Set<String> invalidAddrs = new HashSet<String>();
			String[] ts = invalids.toString().split(";");
			for (String t : ts){
				invalidAddrs.add(t);
			}
			StringBuffer fineAddrs = new StringBuffer();
			ts = mail_targets.split(";");
			for (String t : ts){
				if (!invalidAddrs.contains(t)){
					fineAddrs.append(t + ";");
				}
			}
			if (fineAddrs.length() > 0){
				LogHelper.info("Detect invalid address, will retry to send to:" + fineAddrs.toString());
				ret = sendHTMLOut(fineAddrs.toString(), title, content, filePath, null);
			}else{
				LogHelper.info("Detect invalid address, no valid address ditected abort");
				ret = false;
			}
		}
		return ret;
	}
	
	public boolean sendHTMLOut(String mail_targets, String title, String content, String[] filePath, StringBuffer invalids) {
		// result
		boolean ret = false;
		
		// Get the session object
		Properties props = new Properties();
		props.put("mail.smtp.host", smtp_server);
		props.put("mail.smtp.port", smtp_server_port);
		props.put("mail.smtp.socketFactory.port", smtp_server_port);
		props.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.connectiontimeout", "120000");
		props.put("mail.smtp.timeout", "120000");

		Session session = Session.getDefaultInstance(props,
				new javax.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(smtp_user,
								smtp_password);
					}
				});

		LogHelper.info("About to send mail, title=" + title + ",attachment=" + filePath);
		
		try {
			// Create a default MimeMessage object.
			MimeMessage message = new MimeMessage(session);

			// Set From: header field of the header.
			message.setFrom(new InternetAddress(smtp_user));

			// Set To: header field of the header.
			ArrayList<String> targets = new ArrayList<String>();
			if (mail_targets.contains(";")){
				String[] ts = mail_targets.split(";");
				targets.addAll(Arrays.asList(ts));
			}else{
				targets.add(mail_targets);
			}
			for(String target :targets){
				message.addRecipient(Message.RecipientType.TO, new InternetAddress(target));
			}

			// Set Subject: header field
			message.setSubject(title);
			
			// Create a multipar message
			Multipart multipart = new MimeMultipart();

			// Create the message part
			BodyPart messageContentPart = new MimeBodyPart();
			messageContentPart.setContent(content, "text/html; charset=utf-8");
						
			// Set text message part
			multipart.addBodyPart(messageContentPart);

			if (filePath != null && filePath.length>0){
				for(String path:filePath){
					File file = new File(path);
					if (file.exists()){
						// Part two is attachment
						MimeBodyPart messageattachmentPart = new MimeBodyPart();
						String filename = file.getAbsolutePath();
						DataSource source = new FileDataSource(filename);
						messageattachmentPart.setDataHandler(new DataHandler(source));
						messageattachmentPart.setFileName(file.getName());
						multipart.addBodyPart(messageattachmentPart);
					}
				}
			}

			// Send the complete message parts
			message.setContent(multipart);

			// Send message
			Transport.send(message);
			
			LogHelper.info("Sent out mail, title=" + title);
			
			ret = true;
			
		}catch(SendFailedException sfe){
			Address[] invas = sfe.getInvalidAddresses();
			if (invas != null && invalids != null){
				for (Address adds : invas){
					invalids.append(adds.toString() + ";");
				}
			}
			LogHelper.error("",sfe);
			LogHelper.info("Error for send out mail, title=" + title + ",targets=" + mail_targets + ",invalids=" + (invalids != null ? invalids.toString() : ""));
		}catch (MessagingException mex) {
			LogHelper.error("",mex);
			LogHelper.info("Error for send out mail, title=" + title + ",targets=" + mail_targets);
		}catch(Exception ex){
			LogHelper.error("",ex);
			LogHelper.info("Error for send out mail, title=" + title + ",targets=" + mail_targets);
		}

		return ret;
	}
	public static void main(String[] args) {
		MailSender sender=new MailSender();
		boolean ret=sender.sendHTMLOut("le.xiang@agrant.cn", "test", "test", new String[]{"D:/test.txt","D:/记录.docx"});
		System.out.println(ret);
	}
}
