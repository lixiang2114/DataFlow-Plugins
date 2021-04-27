package com.df.plugin.source.ma.hbase;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.source.ma.hbase.config.HbaseConfig;
import com.df.plugin.source.ma.hbase.service.HbaseService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.ManualPluginAdapter;

/**
 * @author Lixiang
 * @description 基于Hbase分布式存储的离线Source插件
 */
public class HbaseManual extends ManualPluginAdapter {
	/**
	 * HbaseManual配置
	 */
	private HbaseConfig hbaseConfig;
	
	/**
	 * HbaseService服务组件
	 */
	private HbaseService hbaseService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HbaseManual.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("HbaseManual plugin starting...");
		File confFile=new File(pluginPath,"source.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.hbaseConfig=new HbaseConfig(flow).config();
		hbaseService=new HbaseService(hbaseConfig);
		
		return true;
	}
	
	@Override
	public Object handle(Channel<String> sourceToFilterChannel) throws Exception {
		log.info("HbaseManual plugin handing...");
		if(flow.sourceStart) {
			log.info("HbaseManual is already started...");
			return true;
		}
		
		flow.sourceStart=true;
		return hbaseService.startManualETLProcess(sourceToFilterChannel);
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sourceStart=false;
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception {
		log.info("HbaseManual plugin config...");
		if(null==params || 0==params.length) return hbaseConfig.collectRealtimeParams();
		if(params.length<2) return hbaseConfig.getFieldValue((String)params[0]);
		return hbaseConfig.setFieldValue((String)params[0],params[1]);
	}

	/**
	 * 刷新日志文件检查点
	 * @throws IOException
	 */
	@Override
	public Object checkPoint(Object params) throws Exception{
		log.info("HbaseManual plugin reflesh checkpoint...");
		hbaseConfig.refreshCheckPoint();
		return true;
	}
}
