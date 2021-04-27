package com.df.plugin.source.ma.hbase.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.PageFilter;

/**
 * @author Lixiang
 * @description Hbase通用工具集
 */
public class HBaseUtil {
	/**
	 * 字段值最大版本
	 */
	private int maxVersion;
	
	/**
	 * HBase客户端
	 */
	private HBaseAdmin admin;
	
	/**
	 * Hbase客户端连接
	 */
	private Connection connection;
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * Zookeeper主机地址
	 * 支持主机名或IP地址
	 */
	private static final String ZK_HOST_ADDRS="hbase.zookeeper.quorum";
	
	/**
	 * @param hostList 主机地址列表 
	 * @description exam: "ip1:port1,ip2:port2,ip3:port3"
	 */
	public HBaseUtil(String hostList) {
		this(1,hostList);
	}
	
	/**
	 * @param hostList 主机地址列表
	 * @description exam: ["ip1:port1","ip2:port2","ip3:port3"]
	 */
	public HBaseUtil(String... hostList) {
		this(1,String.join(",", hostList));
	}
	
	/**
	 * @param hostList 主机地址列表
	 * @description exam: ["ip1:port1","ip2:port2","ip3:port3"]
	 */
	public HBaseUtil(Iterable<String> hostList) {
		this(1,String.join(",", hostList));
	}
	
	/**
	 * @param maxVersion 字段值最大版本
	 * @param hostList 主机地址列表
	 * @description exam: ["ip1:port1","ip2:port2","ip3:port3"]
	 */
	public HBaseUtil(int maxVersion,String... hostList) {
		this(maxVersion,String.join(",", hostList));
	}
	
	/**
	 * @param maxVersion 字段值最大版本
	 * @param hostList 主机地址列表
	 * @description exam: ["ip1:port1","ip2:port2","ip3:port3"]
	 */
	public HBaseUtil(int maxVersion,Iterable<String> hostList) {
		this(maxVersion,String.join(",", hostList));
	}
	
	/**
	 * @param maxVersion 字段值最大版本
	 * @param hostList 主机地址列表 
	 * @description exam: "ip1:port1,ip2:port2,ip3:port3"
	 */
	private HBaseUtil(int maxVersion,String hostList) {
		Configuration conf = HBaseConfiguration.create();
		conf.set(ZK_HOST_ADDRS, hostList);
		try {
			this.connection=ConnectionFactory.createConnection(conf);
			this.admin=(HBaseAdmin)this.connection.getAdmin();
			this.maxVersion = maxVersion;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除单个列族中的多个字段
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @param family 列族
	 * @param fieldKeys 字段名集
	 * @throws IOException 异常对象
	 */
	public void multiDelete(String tabName,String rowKey,String family,String... fieldKeys) throws IOException {
		Set<String> fieldSet=null;
		if(null!=fieldKeys && 0!=fieldKeys.length) {
			fieldSet=Arrays.stream(fieldKeys)
					.filter(key->null!=key)
					.map(key->key.trim())
					.filter(key->!key.isEmpty())
					.collect(Collectors.toSet());
		}
		
		multiDelete(tabName,rowKey,Collections.singletonMap(family, fieldSet));
	}
	
	/**
	 * 删除单条记录或记录中的多个字段
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @param familyMap 列族字典
	 * @throws IOException 异常对象
	 */
	public void multiDelete(String tabName,String rowKey,Map<String,Set<String>> familyMap) throws IOException {
		multiDelete(Collections.singletonMap(tabName, Collections.singletonMap(rowKey, familyMap)));
	}
	
	/**
	 * 删除多条记录或记录中的多个字段
	 * @param tabMap 表级字典
	 * @throws IOException 异常对象
	 * @description 若行键rowKey对应的键值为NULL或空则删除整行,否则删除行内指定键的值对
	 */
	public void multiDelete(Map<String,Map<String,Map<String,Set<String>>>> tabMap) throws IOException {
		if(null==tabMap || tabMap.isEmpty()) return;
		Map<String, Map<String, Set<String>>> rowMap=null;
		for(Entry<String, Map<String, Map<String, Set<String>>>> entry:tabMap.entrySet()) {
			if(null==(rowMap=entry.getValue())) continue;
			TableName tabName=TableName.valueOf(entry.getKey());
			if(!admin.tableExists(tabName)) continue;
			Table table=connection.getTable(tabName);
			for(Entry<String, Map<String, Set<String>>> rowEntry:rowMap.entrySet()) {
				byte[] rowKey=rowEntry.getKey().getBytes();
				Get get=new Get(rowKey);
				Delete delete=new Delete(rowKey);
				Map<String, Set<String>> familyMap=rowEntry.getValue();
				if(null==familyMap || familyMap.isEmpty()) {
					table.delete(delete);
					continue;
				}
				
				Set<String> fieldSet=null;
				for(Entry<String, Set<String>> familyEntry:familyMap.entrySet()) {
					byte[] family=familyEntry.getKey().getBytes();
					if(null==(fieldSet=familyEntry.getValue())) continue;
					for(String fieldKey:fieldSet) {
						byte[] key=fieldKey.getBytes();
						get.addColumn(family, key);
						if(!table.exists(get)) continue;
						delete.addColumn(family,key);
						byte[] b=table.get(get).getValue(family,key);		
						while(null!=b&&b.length>0) {
							table.delete(delete);
							b=table.get(get).getValue(family,key);
						}
					}
				}
			}
			table.close();
		}
	}
	
	/**
	 * 批量删除指定表中指定行键表对应的多条记录
	 * @param tabName 表名
	 * @param rowKeys 行键表
	 * @throws IOException 异常对象
	 */
	public void batchDelete(String tabName,String... rowKeys) throws IOException {
		if(null==rowKeys || 0==rowKeys.length) return;
		batchDelete(Collections.singletonMap(tabName, rowKeys));
	}
	
	/**
	 * 批量删除记录
	 * @param tabMap 表级字典
	 * @throws IOException 异常对象
	 * @description tabMap exam: Map<tabName,[rowKey1,rowKey2...]>
	 */
	public void batchDelete(Map<String,String[]> tabMap) throws IOException {
		if(null==tabMap || tabMap.isEmpty()) return;
		for(Entry<String,String[]> entry:tabMap.entrySet()) {
			TableName tabName=TableName.valueOf(entry.getKey());
			if(!admin.tableExists(tabName)) continue;
			
			String[] rowKeys=entry.getValue();
			if(null==rowKeys || 0==rowKeys.length) return;
			
			Set<String> deleteKeys=Arrays.stream(rowKeys)
					.filter(rowKey->null!=rowKey)
					.map(rowKey->rowKey.trim())
					.filter(rowKey->!rowKey.isEmpty())
					.collect(Collectors.toSet());
			
			if(null==deleteKeys || deleteKeys.isEmpty()) return;
			
			List<Delete> deletes=deleteKeys.stream()
					.map(rowKey->new Delete(rowKey.getBytes()))
					.collect(Collectors.toList());
			
			Table table = connection.getTable(tabName);
			table.delete(deletes);
			table.close();
		}
	}
	
	/**
	 * 向指定行中批量插入或修改多个列族值
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @param family 列族
	 * @param fieldKey 字段名
	 * @param fieldValue 字段值
	 * @throws Exception 抛出异常对象
	 */
	public void put(String tabName,String rowKey,String family,String fieldKey,Object fieldValue) throws Exception {
		put(tabName,rowKey,family,Collections.singletonMap(fieldKey, fieldValue));
	}
	
	/**
	 * 向指定行中批量插入或修改多个列族值
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @param family 列族
	 * @param fieldMap 字段字典
	 * @throws Exception 抛出异常对象
	 * @description fieldMap exam: Map<key,value>
	 */
	public void put(String tabName,String rowKey,String family,Map<String,Object> fieldMap) throws Exception {
		put(tabName,rowKey,Collections.singletonMap(family, fieldMap));
	}
	
	/**
	 * 向指定行中批量插入或修改多个列族值
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @param familyMap 列族字典
	 * @throws Exception 抛出异常对象
	 * @description familyMap exam: Map<family,Map<key,value>>
	 */
	public void put(String tabName,String rowKey,Map<String,Map<String,Object>> familyMap) throws Exception {
		batchPut(tabName,Collections.singletonMap(rowKey, familyMap));
	}
	
	/**
	 * 向指定表中批量插入或修改多条记录
	 * @param tabName 表名
	 * @param rowMap 行级字典
	 * @throws Exception 抛出异常对象
	 * @description rowMap exam: Map<rowKey,Map<family,Map<key,value>>>
	 */
	public void batchPut(String tabName,Map<String,Map<String,Map<String,Object>>> rowMap) throws Exception {
		batchPut(Collections.singletonMap(tabName, rowMap));
	}
	
	/**
	 * 批量插入或修改记录
	 * @param tabMap 表级字典
	 * @throws Exception 抛出异常对象
	 * @description tabMap exam: Map<tabName,Map<rowKey,Map<family,Map<key,value>>>>
	 */
	public void batchPut(Map<String,Map<String,Map<String,Map<String,Object>>>> tabMap,boolean... autoCreateTab) throws Exception{
		if(null==tabMap || tabMap.isEmpty()) return;
		boolean autoCreate=null==autoCreateTab || 0==autoCreateTab.length?true:autoCreateTab[0];
		for(Entry<String, Map<String, Map<String, Map<String, Object>>>> entry:tabMap.entrySet()) {
			String tabName=entry.getKey();
			TableName tableName=TableName.valueOf(tabName);
			if(!admin.tableExists(tableName)) {
				if(!autoCreate) return;
				Map<String, Map<String, Map<String, Object>>> rowMap=entry.getValue();
				if(null==rowMap || rowMap.isEmpty()) return;
				Map<String, Map<String, Object>> familyMap=rowMap.values().iterator().next();
				if(null==familyMap || familyMap.isEmpty()) return;
				Set<String> familySet=familyMap.keySet();
				if(null==familySet || familySet.isEmpty()) return;
				System.out.println("HBaseUtil create table: "+tabName+",familySet: "+familySet+",maxVersion: "+maxVersion);
				createTable(tabName,familySet,maxVersion);
			}
			
			Table table=connection.getTable(tableName);
			List<Put> puts=new ArrayList<Put>();
			for(Entry<String, Map<String, Map<String, Object>>> rowEntry:entry.getValue().entrySet()){
				Put put=new Put(rowEntry.getKey().getBytes());
				for(Entry<String, Map<String, Object>> familyEntry:rowEntry.getValue().entrySet()){
					byte[] family=familyEntry.getKey().getBytes();
					for(Entry<String, Object> fieldEntry:familyEntry.getValue().entrySet()){
						put.addColumn(family, fieldEntry.getKey().getBytes(), getBytes(fieldEntry.getValue()));
					}
				}
				puts.add(put);
			}
			table.put(puts);
			table.close();
		}
	}
	
	/**
	 * 根据行键/列族/字段名获取单个值
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @param family 列族
	 * @param fieldKey 字段名
	 * @return 字段值
	 * @throws Exception 异常对象
	 */
	public Object getValue(String tabName,String rowKey,String family,String fieldKey) throws Exception{
		TableName tableName=TableName.valueOf(tabName);
		if(!admin.tableExists(tableName)) return null;
		
		Table table=connection.getTable(tableName);
		Result result=table.get(new Get(rowKey.getBytes()));
		byte[] byteValue=result.getValue(family.getBytes(), fieldKey.getBytes());
		
		table.close();
		return getObject(byteValue);
	}
	
	/**
	 * 根据行键和列族名查询单个列族数据
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @param family 列族
	 * @return 列族对象(字段字典)
	 * @throws Exception
	 */
	public HashMap<String,Object> getFamily(String tabName,String rowKey,String family) throws Exception{
		TableName tableName=TableName.valueOf(tabName);
		if(!admin.tableExists(tableName)) return null;
		
		Table table=connection.getTable(tableName);
		Result result=table.get(new Get(rowKey.getBytes()));
		
		NavigableMap<byte[], byte[]> resultMap=result.getFamilyMap(family.getBytes());
		if(null==resultMap || resultMap.isEmpty()) return null;
		
		HashMap<String,Object> fieldMap=new HashMap<String,Object>();
		for(Entry<byte[],byte[]> entry:resultMap.entrySet()) fieldMap.put(new String(entry.getKey()), getObject(entry.getValue()));
		return fieldMap;
	}
	
	/**
	 * 根据行键rowKey查询单条记录
	 * @param tabName 表名
	 * @param rowKey 行键
	 * @return 记录对象(列族字典)
	 * @throws Exception
	 */
	public Map<String,Map<String,Object>> getRecord(String tabName,String rowKey) throws Exception{
		TableName tableName=TableName.valueOf(tabName);
		if(!admin.tableExists(tableName)) return null;
		
		Table table=connection.getTable(tableName);
		Result result=table.get(new Get(rowKey.getBytes()));
		
		List<Cell> cells=result.listCells();
		Map<String,Object> fieldMap=null;
		Map<String,Map<String,Object>> familyMap=new HashMap<String,Map<String,Object>>();
		for(Cell cell:cells){
			String family=new String(CellUtil.cloneFamily(cell));
			String fieldKey=new String(CellUtil.cloneQualifier(cell));
			Object fieldValue=getObject(CellUtil.cloneValue(cell));
			fieldMap=familyMap.get(family);
			if(null==fieldMap) familyMap.put(family, fieldMap=new HashMap<String,Object>());
			fieldMap.put(fieldKey, fieldValue);
		}
		return familyMap;
	}
	
	/**
	 * 分页扫描指定表中的记录
	 * @param tabName 表名称
	 * @param startRowKey 起始行键
	 * @param countLimit 页面记录数量
	 * @throws Exception 抛出异常对象
	 * @return 有序行字典表
	 */
	public LinkedHashMap<String, Map<String, Map<String, Object>>> pageGet(String tabName,String startRowKey,long countLimit) throws Exception{
		TableName tableName=TableName.valueOf(tabName);
		if(!admin.tableExists(tableName)) return null;
		
		Scan scan=new Scan();
		scan.withStartRow(startRowKey.getBytes());
		scan.setFilter(new PageFilter(countLimit));
		
		Table table=connection.getTable(tableName);
		ResultScanner ress=table.getScanner(scan);
		if(null==ress) return null;
		
		LinkedHashMap<String, Map<String, Map<String, Object>>> rowMap=getRowListByScanner(ress);
		table.close();
		return rowMap;
	}
	
	/**
	 * 关闭Hbase服务连接
	 * @throws Exception
	 */
	public void close() throws IOException{
		if(null!=admin) admin.close();
		if(null!=connection) connection.close();
	}
	
	/**
	 * 将查询结果集包装为List集合返回
	 * @param ress 结果集迭代器
	 * @return 记录列表
	 * @description 返回记录列表,exam:Map<rowKey,Map<family,Map<key,value>>>
	 */
	private LinkedHashMap<String,Map<String,Map<String,Object>>> getRowListByScanner(ResultScanner ress) throws Exception{
		LinkedHashMap<String,Map<String,Map<String,Object>>> rowMap=new LinkedHashMap<String,Map<String,Map<String,Object>>>();
		for(Result res:ress) {
			String rowKey=new String(res.getRow());
			Map<String, Map<String, Object>> familyMap=rowMap.get(rowKey);
			if(null==familyMap) rowMap.put(rowKey, familyMap=new HashMap<String, Map<String, Object>>());
			List<Cell> cells=res.listCells();
			for(Cell cell:cells) {
				String family=new String(CellUtil.cloneFamily(cell));
				String fieldKey=new String(CellUtil.cloneQualifier(cell));
				Object fieldValue=getObject(CellUtil.cloneValue(cell));
				Map<String,Object> fieldMap=familyMap.get(family);
				if(null==fieldMap) familyMap.put(family, fieldMap=new HashMap<String,Object>());
				fieldMap.put(fieldKey, fieldValue);
			}
		}
		return rowMap;
	}
	
	/**
	 * 创建库表
	 * @param tabName 库表名
	 * @return true:调用后表存在,false:调用后无影响
	 * @throws Exception 异常对象
	 */
	private boolean createTable(String tabName,Set<String> familySet,int maxVersion) throws Exception {
		if(null==tabName) return false;
		if((tabName=tabName.trim()).isEmpty()) return false;
		if(null==familySet || familySet.isEmpty()) return false;
		
		TableName tableName=TableName.valueOf(tabName);
		if(admin.tableExists(tableName)) return true;
		
		String[] tsAndTab=COLON_REGEX.split(tabName);
		if(2>tsAndTab.length) return false;
		
		String tableSpaceStr=tsAndTab[0].trim();
		String tableNameStr=tsAndTab[1].trim();
		if(tableSpaceStr.isEmpty() || tableNameStr.isEmpty()) return false;
		
		tableName=TableName.valueOf(new StringBuilder(tableSpaceStr).append(":").append(tableNameStr).toString());
		if(admin.tableExists(tableName)) return true;
		
		if(!ArrayUtils.contains(admin.listNamespaces(), tableSpaceStr)) admin.createNamespace(NamespaceDescriptor.create(tableSpaceStr).build());
		if(!admin.tableExists(tableName)) createTable(tableName,familySet,maxVersion);
		return true;
	}
	
	/**
	 * 创建HBase表定义
	 * @param tabName 库表名,如:"wa:tuser"
	 * @param familySet 列族集,如:{baseinfo,extrainfo}
	 * @param maxVersion 字段最大版本
	 * @throws IOException
	 */
	private void createTable(TableName tableName,Set<String> familySet,int maxVersion) throws IOException{
		TableDescriptorBuilder tableDescriptorBuilder=TableDescriptorBuilder.newBuilder(tableName);
		ColumnFamilyDescriptor columnFamilyDescriptor=null;
		for(String family:familySet){
			columnFamilyDescriptor=ColumnFamilyDescriptorBuilder.newBuilder(family.getBytes()).setMaxVersions(maxVersion).setMinVersions(1).build();
			tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
		}
		admin.createTable(tableDescriptorBuilder.build());
	}
	
	/**
	 * 将参数对象序列化为字节数组
	 * @param object JAVA对象
	 * @return 字节数组
	 * @throws Exception
	 */
	private static byte[] getBytes(Object object) throws Exception {
		if(null==object)return null;
		ByteArrayOutputStream baos=new ByteArrayOutputStream();
		new ObjectOutputStream(baos).writeObject(object);
		return baos.toByteArray();
	}
	
	/**
	 * 将参数字节反序列化为对象
	 * @param bytes 字节数组
	 * @return JAVA对象
	 * @throws Exception
	 */
	private static Object getObject(byte[] bytes) throws Exception {
		if(null==bytes || 0==bytes.length)return null;
		return new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
	}
}
