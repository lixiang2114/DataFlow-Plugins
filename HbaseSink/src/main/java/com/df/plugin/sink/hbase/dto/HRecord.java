package com.df.plugin.sink.hbase.dto;

import com.google.common.base.Objects;

/**
 * @author Lixiang
 * @description Hbase记录映射器
 */
public class HRecord {
	/**
	 * 库表名称
	 */
	public String tabName;
	
	/**
	 * 行键名
	 */
	public String rowKey;
	
	/**
	 * 列族名
	 */
	public String family;
	
	/**
	 * 字段名
	 */
	public String fieldKey;
	
	/**
	 * 字段值
	 */
	public Object fieldValue;
	
	public HRecord(String tabName,String rowKey,String family,String fieldKey,Object fieldValue) {
		this.family=family;
		this.rowKey=rowKey;
		this.fieldKey=fieldKey;
		this.tabName=tabName;
		this.fieldValue=fieldValue;
	}
	
	/**
	 * 获取坐标定位键
	 * @return 坐标定位键
	 */
	public String getLocationKey() {
		return new StringBuilder(tabName).append(".").append(family).append(".").append(rowKey).append(".").append(fieldKey).toString();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(tabName,family,rowKey,fieldKey,fieldValue);
	}

	@Override
	public boolean equals(Object obj) {
		if(null==obj) return false;
		if(!(obj instanceof HRecord)) return false;
		
		HRecord record=((HRecord)obj);
		if(getLocationKey().equals(record.getLocationKey()) && fieldValue.equals(record.fieldValue)) return true;
		
		return false;
	}

	@Override
	public String toString() {
		return new StringBuilder(getLocationKey()).append(":").append(fieldValue).toString();
	}
}
