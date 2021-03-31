package com.df.plugin.source.sql.service;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.sql.config.SqlConfig;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.util.CommonUtil;

/**
 * @author Lixiang
 * @description 数据库服务模块
 */
@SuppressWarnings("unchecked")
public class SqlService {
	/**
	 * SqlSource配置
	 */
	private SqlConfig sqlConfig;
	
	/**
	 * 查询拉取SQL语句
	 */
	private PreparedStatement selectStat;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlService.class);
	
	public SqlService(){}
	
	public SqlService(SqlConfig sqlConfig){
		this.sqlConfig=sqlConfig;
	}
	
	/**
	 * 停止ETL流程
	 */
	public Boolean stopManualETLProcess(Object params) {
		log.info("stop source ETL process...");
		sqlConfig.flow.sourceStart=false;
		return true;
	}

	/**
	 * 启动ETL流程(读取预置SQL数据库数据)
	 */
	public Object startManualETLProcess(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("execute sql source process...");
		ConcurrentHashMap<String, Integer> pageDict=sqlConfig.pageDict;
		if(null==selectStat || selectStat.isClosed()) selectStat=sqlConfig.getConnection().prepareStatement(sqlConfig.selectSQL);
		
		ResultSet res=null;
		String[] phFields=sqlConfig.phFields;
		int batchSize=pageDict.get("batchSize");
		
		try{
			while(sqlConfig.flow.sourceStart){
				for(int i=0;i<phFields.length;selectStat.setObject(i+1, pageDict.get(phFields[i])),i++);
				res=selectStat.executeQuery();
				ResultSetMetaData rsmd=res.getMetaData();
				int fieldNum=rsmd.getColumnCount();
				int counter=0;
				for(;res.next();counter++) {
					switch(sqlConfig.outFormat) {
						case "qstr":
							StringBuilder recordQuery=new StringBuilder("");
							for(int i=0;i<fieldNum;i++){
								if(java.util.Date.class.isAssignableFrom(Class.forName(rsmd.getColumnClassName(i+1)))) {
									recordQuery.append(rsmd.getColumnLabel(i+1).trim()).append("=").append(CommonUtil.transferType(res.getObject(i+1), String.class)).append("&");
								}else{
									recordQuery.append(rsmd.getColumnLabel(i+1).trim()).append("=").append(res.getObject(i+1)).append("&");
								}
							}
							if(0==recordQuery.length()) continue;
							sourceToFilterChannel.put(recordQuery.deleteCharAt(recordQuery.length()-1).toString());
							break;
						case "map":
							HashMap<String,Object> recordMap=new HashMap<String,Object>();
							for(int i=0;i<fieldNum;i++) {
								if(java.util.Date.class.isAssignableFrom(Class.forName(rsmd.getColumnClassName(i+1)))) {
									recordMap.put(rsmd.getColumnLabel(i+1).trim(), CommonUtil.transferType(res.getObject(i+1), String.class));
								}else{
									recordMap.put(rsmd.getColumnLabel(i+1).trim(), res.getObject(i+1));
								}
							}
							sourceToFilterChannel.put(CommonUtil.javaToJsonStr(recordMap));
							break;
						default:
							StringBuilder recordValues=new StringBuilder("");
							for(int i=0;i<fieldNum;i++){
								if(java.util.Date.class.isAssignableFrom(Class.forName(rsmd.getColumnClassName(i+1)))) {
									recordValues.append(CommonUtil.transferType(res.getObject(i+1), String.class)).append(",");
								}else{
									recordValues.append(res.getObject(i+1)).append(",");
								}
							}
							if(0==recordValues.length()) continue;
							sourceToFilterChannel.put(recordValues.deleteCharAt(recordValues.length()-1).toString());
					}
				}
				
				if(null!=res) res.close();
				selectStat.clearParameters();
				if(counter<batchSize && !sqlConfig.realtime) break;
				
				pageDict.put("startIndex", pageDict.get("startIndex")+counter);
				pageDict.put("endIndex", pageDict.get("endIndex")+counter);
				if(counter>=batchSize) continue;
				Thread.sleep(2000L);
			}
			
			log.info("SqlSource plugin etl process normal exit,execute checkpoint...");
		}catch(Exception e){
			log.info("SqlSource plugin etl process occur Error...",e);
		}finally{
			try {
				sqlConfig.refreshCheckPoint();
			} catch (IOException e) {
				log.error("SqlSource call refreshCheckPoint occur Error...",e);
			}
			
			try{
				if(null!=res) res.close();
				if(null!=selectStat) selectStat.close();
			}catch(SQLException e){
				log.error("SqlSource close sql stream occur Error...",e);
			}
		}
		return true;
	}
}
