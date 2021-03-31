package com.df.plugin.extend.filter;

import java.io.File;
import java.lang.reflect.Array;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.df.plugin.extend.filter.config.FilterConfig;
import com.df.plugin.extend.filter.util.DyScriptUtil;
import com.github.lixiang2114.flow.comps.Channel;
import com.github.lixiang2114.flow.plugins.adapter.FilterPluginAdapter;

/**
 * @author Lixiang
 * @description 扩展过滤器
 */
public class ExtendFilter extends FilterPluginAdapter{
	/**
	 * 过滤器配置
	 */
	private FilterConfig filterConfig;
	
	/**
	 * 日志工具
	 */
	private static final Logger log=LoggerFactory.getLogger(ExtendFilter.class);
	
	@Override
	public Boolean init() throws Exception {
		log.info("ExtendFilter plugin starting...");
		File confFile=new File(pluginPath,"filter.properties");
		if(!confFile.exists()) {
			log.error(confFile.getAbsolutePath()+" is not exists...");
			return false;
		}
		
		this.filterConfig=new FilterConfig(flow).config();
		return true;
	}

	@Override
	public Object filter(Channel<String> sourceToFilterChannel,Channel<String> filterToSinkChannel) throws Exception{
		log.info("ExtendFilter plugin filtering...");
		if(flow.filterStart) {
			log.info("ExtendFilter is already started...");
			return true;
		}
		
		flow.filterStart=true;
		String mainClass=filterConfig.mainClass;
		String mainMethod=filterConfig.mainMethod;
		try{
			String message=null;
			if(filterConfig.through) {
				while (this.flow.filterStart) {
					if (null==(message=sourceToFilterChannel.get())) continue;
					if((message=message.trim()).isEmpty()) continue;
					filterToSinkChannel.put(message);
				}
			}else{
				while(flow.filterStart) {
					if(null==(message=sourceToFilterChannel.get())) continue;
					if((message=message.trim()).isEmpty()) continue;
					
					Object result=DyScriptUtil.execFunc(mainClass, mainMethod, message);
					if(null==result) continue;
					
					if(!result.getClass().isArray()) {
						String outLine=result.toString().trim();
						if(!outLine.isEmpty()) filterToSinkChannel.put(outLine);
						continue;
					}
					
					int len=Array.getLength(result);
					for(int i=0;i<len;i++) {
						Object object=Array.get(result, i);
						if(null==object) continue;
						String outLine=object.toString().trim();
						if(outLine.isEmpty()) continue;
						filterToSinkChannel.put(outLine);
					}
				}
			}
		}catch(InterruptedException e){
			log.warn("filter plugin is interrupted while waiting...");
		}
		
		return true;
	}

	@Override
	public Object stop(Object params) throws Exception {
		flow.filterStart=false;
		return true;
	}
	
	@Override
	public Object config(Object... params) throws Exception {
		log.info("ExtendFilter plugin config...");
		if(null==params || 0==params.length) return filterConfig.collectRealtimeParams();
		if(params.length<2) return filterConfig.getFieldValue((String)params[0]);
		return filterConfig.setFieldValue((String)params[0],params[1]);
	}
}
