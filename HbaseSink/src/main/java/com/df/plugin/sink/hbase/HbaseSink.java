package com.df.plugin.sink.hbase;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.sink.hbase.config.HbaseConfig;
import com.df.plugin.sink.hbase.service.HbaseService;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.SinkPluginAdapter;

/**
 * @author Lixiang
 * @description Hbase发送器
 */
public class HbaseSink extends SinkPluginAdapter{
	/**
	 * Hbase写出配置
	 */
	private HbaseConfig hbaseConfig;
	
	/**
	 * Hbase服务配置
	 */
	private HbaseService hbaseService;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(HbaseSink.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("HbaseSink plugin starting...");
		File confFile=new File(pluginPath,"sink.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.hbaseConfig=new HbaseConfig(flow).config();
		this.hbaseService=new HbaseService(hbaseConfig);
		
		return true;
	}

	@Override
	public Object send(Channel<String> filterToSinkChannel) throws Exception {
		log.info("HbaseSink plugin handing...");
		if(flow.sinkStart) {
			log.info("HbaseSink is already started...");
			return true;
		}
		
		flow.sinkStart=true;
		
		try{
			long batchMills=hbaseConfig.batchMaxWaitMills;
			if(hbaseConfig.parse) {
				while(flow.sinkStart) {
					Boolean flag=hbaseService.parseSend(filterToSinkChannel.get(batchMills));
					if(null!=flag && !flag) return false;
				}
			}else{
				while(flow.sinkStart) {
					Boolean flag=hbaseService.noParseSend(filterToSinkChannel.get(batchMills));
					if(null!=flag && !flag) return false;
				}
			}
		}catch(Exception e){
			log.warn("sink plugin handing occur error",e);
		}
		
		return true;
	}
	
	@Override
	public Object stop(Object params) throws Exception {
		flow.sinkStart=false;
		hbaseService.stop();
		return true;
	}

	@Override
	public Object config(Object... params) throws Exception{
		log.info("HbaseSink plugin config...");
		if(null==params || 0==params.length) return hbaseConfig.collectRealtimeParams();
		if(params.length<2) return hbaseConfig.getFieldValue((String)params[0]);
		return hbaseConfig.setFieldValue((String)params[0],params[1]);
	}
}
