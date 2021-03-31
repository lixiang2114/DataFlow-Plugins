package com.df.plugin.sink.sql;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.sql.config.SqlConfig;
import com.df.plugin.sink.sql.service.SqlService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description SQL发送器
 * 通用关系型数据库发送器
 */
public class SqlSink extends SinkPluginAdapter{
	/**
	 * SQL客户端配置
	 */
	private SqlConfig sqlConfig;
	
	/**
	 * SQL服务
	 */
	private SqlService sqlService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(SqlSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("SqlSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.sqlConfig=new SqlConfig(flow).config();
		this.sqlService=new SqlService(sqlConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("SqlSink plugin handing...");
		if(flow.sinkStart) {
			log.info("SqlSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		if(!sqlService.preSend()) return false;
		
		try{
			String message=null;
			if(sqlConfig.parse){
				if(null==sqlConfig.batchSize) {
					while(flow.sinkStart) {
						if(null==(message=filterToSinkChannel.get())) continue;
						if((message=message.trim()).isEmpty()) continue;
						Boolean flag=sqlService.parseAndSingleSend(message);
						if(null!=flag && !flag) return false;
					}
				}else{
					while(flow.sinkStart) {
						Boolean flag=sqlService.parseAndBatchSend(filterToSinkChannel.get(sqlConfig.batchMaxWaitMills));
						if(null!=flag && !flag) return false;
					}
				}
			}else{
				if(null==sqlConfig.batchSize) {
					while(flow.sinkStart) {
						if(null==(message=filterToSinkChannel.get())) continue;
						if((message=message.trim()).isEmpty()) continue;
						Boolean flag=sqlService.noParseAndSingleSend(message);
						if(null!=flag && !flag) return false;
					}
				}else{
					while(flow.sinkStart) {
						Boolean flag=sqlService.noParseAndBatchSend(filterToSinkChannel.get(sqlConfig.batchMaxWaitMills));
						if(null!=flag && !flag) return false;
					}
				}
			}
		}catch(InterruptedException e){
			log.warn("sink plugin is interrupted while waiting...");
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("SqlSink plugin config...");
		if(null==params || 0==params.length) return sqlConfig.collectRealtimeParams();
		if(params.length<2) return sqlConfig.getFieldValue((String)params[0]);
		return sqlConfig.setFieldValue((String)params[0],params[1]);
	}
}
