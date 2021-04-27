package com.df.plugin.sink.hbase.dto;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

/**
 * @author Lixiang
 * @description 通道字段映射器
 */
public class FieldMapper {
	/**
	 * 解析字段数组
	 */
	private String[] parseFields;
	
	/**
	 * 解析字段键定义表
	 */
	private Set<String> keyDefines;
	
	/**
	 * 库表字段映射字典
	 */
	private Map<String,String> fieldMap;
	
	public FieldMapper(Map<String,String> fieldMap,String... keyDefines) {
		if(null==keyDefines || 0==keyDefines.length) {
			throw new RuntimeException("keyDefines can not be empty...");
		}
		
		Set<String> keyDefineSet=fieldMap.keySet();
		for(String keyDefine:keyDefines) {
			if(keyDefineSet.contains(keyDefine)) continue;
			throw new RuntimeException("fieldMap must contains all fieldKey for keyDefines: "+Arrays.toString(keyDefines));
		}
		
		this.keyDefines=Arrays.stream(keyDefines).collect(Collectors.toSet());
		this.fieldMap=fieldMap;
	}
	
	/**
	 * 设置解析字段表
	 * @param parseFields 解析字段表
	 */
	public void setParseFields(String... parseFields) {
		if(null==parseFields || 0==parseFields.length) return;
		
		for(String defineKey:keyDefines) {
			String fieldName=fieldMap.get(defineKey);
			if(ArrayUtils.contains(parseFields, fieldName)) continue;
			throw new RuntimeException("parseFields must contains fieldName: "+fieldName);
		}
		
		this.parseFields = parseFields;
	}

	/**
	 * 解析数据序列中指定键定义的字段值
	 * @param defineKey 字段名定义键
	 * @param valueArray 数据值序列
	 * @return 字段值
	 */
	public String parseValue(String defineKey,String[] valueArray) {
		if(null==parseFields) return null;
		String keyName=fieldMap.get(defineKey);
		int keyIndex=ArrayUtils.indexOf(parseFields, keyName);
		return valueArray[keyIndex].trim();
	}

	@Override
	public String toString() {
		LinkedHashMap<String,Object> strMap=new LinkedHashMap<String,Object>();
		strMap.put("keyDefines", keyDefines);
		strMap.put("fieldMap", fieldMap);
		strMap.put("parseFields", Arrays.toString(parseFields));
		return strMap.toString();
	}
}
