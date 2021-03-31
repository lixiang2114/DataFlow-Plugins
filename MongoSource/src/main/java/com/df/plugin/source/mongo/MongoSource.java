package com.df.plugin.source.mongo;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.mongo.config.MdbConfig;
import com.df.plugin.source.mongo.service.MdbService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.ManualPluginAdapter;

/**
 * @author Lixiang
 * @description MDB数据库Source插件
 * 本插件不会删除原始数据表中的记录
 */
public class MongoSource extends ManualPluginAdapter {
	/**
	 * MongoSource配置
	 */
	private MdbConfig mdbConfig;
	
	/**
	 * MongoService服务模块
	 */
	private MdbService mdbService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(MongoSource.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("MongoSource plugin starting...");
		File confFile=new File(pluginPath,"source.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.mdbConfig=new MdbConfig(flow).config();
		this.mdbService=new MdbService(mdbConfig);
		
		return true;
	}
	
	@Override
	public Object handle(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("MongoSource plugin handing...");
		if(flow.sourceStart) {
			log.info("MongoSource is already started...");
			return true;
		}
		
		flow.sourceStart=true;
		return mdbService.startManualETLProcess(sourceToFilterChannel);
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sourceStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("MongoSource plugin config...");
		if(null==params || 0==params.length) return mdbConfig.collectRealtimeParams();
		if(params.length<2) return mdbConfig.getFieldValue((String)params[0]);
		return mdbConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新数据记录检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.info("MongoSource plugin reflesh checkpoint...");
		mdbConfig.refreshCheckPoint();
		return true;
	}
}
