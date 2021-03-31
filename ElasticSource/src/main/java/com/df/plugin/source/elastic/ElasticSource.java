package com.df.plugin.source.elastic;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.elastic.config.ElasticConfig;
import com.df.plugin.source.elastic.service.ElasticService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.ManualPluginAdapter;

/**
 * @author Lixiang
 * @description Elastic存储Source插件
 * 本插件不会删除原始索引库中的记录
 */
public class ElasticSource extends ManualPluginAdapter {
	/**
	 * ElasticSource配置
	 */
	private ElasticConfig elasticConfig;
	
	/**
	 * ElasticService服务模块
	 */
	private ElasticService elasticService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ElasticSource.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("ElasticSource plugin starting...");
		File confFile=new File(pluginPath,"source.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.elasticConfig=new ElasticConfig(flow).config();
		this.elasticService=new ElasticService(elasticConfig);
		
		return true;
	}
	
	@Override
	public Object handle(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("ElasticSource plugin handing...");
		if(flow.sourceStart) {
			log.info("ElasticSource is already started...");
			return true;
		}
		
		flow.sourceStart=true;
		return elasticService.startManualETLProcess(sourceToFilterChannel);
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		return elasticService.stopManualETLProcess(params);
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("ElasticSource plugin config...");
		if(null==params || 0==params.length) return elasticConfig.collectRealtimeParams();
		if(params.length<2) return elasticConfig.getFieldValue((String)params[0]);
		return elasticConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新数据记录检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.info("ElasticSource plugin reflesh checkpoint...");
		elasticConfig.refreshCheckPoint();
		return true;
	}
}
