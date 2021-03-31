package com.df.plugin.sink.flow.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * @author Lixiang
 * @description ZIP压缩工具类
 */
public class ZipUtil {
	/**
     * 执行数据压缩处理(压缩格式:GZIP/ZLIB)
     * @param data 被压缩的数据
     * @return 压缩后的数据
     * @throws IOException
     */
    public static byte[] compress(byte[] data) throws IOException {
    	return compress(data,Deflater.DEFAULT_COMPRESSION,true);
    }
    
    /**
     * 执行数据压缩处理(压缩格式:GZIP/ZLIB)
     * @param data 被压缩的数据
     * @return 压缩后的数据
     * @throws IOException
     */
    public static String compressToString(String data) throws IOException {
    	return compressToString(data.getBytes());
    }
    
    /**
     * 执行数据压缩处理(压缩格式:GZIP/ZLIB)
     * @param data 被压缩的数据
     * @param charset 字符集编码
     * @return 压缩后的数据
     * @throws IOException
     */
    public static String compressToString(String data,Charset charset) throws IOException {
    	return compressToString(data.getBytes(charset));
    }
    
    /**
     * 执行数据压缩处理(压缩格式:GZIP/ZLIB)
     * @param data 被压缩的数据
     * @return 压缩后的数据
     * @throws IOException
     */
    public static String compressToString(byte[] data) throws IOException {
    	return Base64.getEncoder().encodeToString(compress(data,Deflater.DEFAULT_COMPRESSION,true));
    }
    
    /**
     * 执行数据压缩处理(压缩格式:GZIP/ZLIB)
     * @param data 被压缩的数据
     * @param level 压缩级别(0-9)
     * @return 压缩后的数据
     * @throws IOException
     */
    public static byte[] compress(byte[] data,Integer level) throws IOException {
    	return compress(data,level,true);
    }
    
    /**
     * 执行数据压缩处理(压缩格式:GZIP/ZLIB)
     * @param data 被压缩的数据
     * @param nowrap 是否包装压缩头(如校验和:checksum)
     * @return 压缩后的数据
     * @throws IOException
     */
    public static byte[] compress(byte[] data,Boolean nowrap) throws IOException {
    	return compress(data,Deflater.DEFAULT_COMPRESSION,nowrap);
    }
    
    /**
     * 执行数据压缩处理(压缩格式:GZIP/ZLIB)
     * @param data 被压缩的数据
     * @param level 压缩级别(0-9)
     * @param nowrap 是否包装压缩头(如校验和:checksum)
     * @return 压缩后的数据
     * @throws IOException
     */
    public static byte[] compress(byte[] data,Integer level,Boolean nowrap) throws IOException {
    	if(null==nowrap) nowrap=true;
    	if(null==level) level=Deflater.DEFAULT_COMPRESSION;
    	
        byte[] buf = new byte[1024];
        Deflater compress = new Deflater(level, nowrap);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        compress.reset();
        compress.finish();
        compress.setInput(data);
        try {
            while (!compress.finished()) baos.write(buf, 0, compress.deflate(buf));
            return baos.toByteArray();
        } finally {
        	baos.close();
            compress.end();
        }
    }
    
    /**
     * 执行数据解压处理(解压格式:GZIP/ZLIB)
     * @param data 被解压的数据
     * @return 解压后的数据
     * @throws IOException
     * @throws DataFormatException 
     */
    public static String uncompressFromString(String data) throws IOException, DataFormatException {
    	return new String(uncompress(Base64.getDecoder().decode(data)));
    }
    
    /**
     * 执行数据解压处理(解压格式:GZIP/ZLIB)
     * @param data 被解压的数据
     * @param charset 字符编码集
     * @return 解压后的数据
     * @throws IOException
     * @throws DataFormatException 
     */
    public static String uncompressFromString(String data,Charset charset) throws IOException, DataFormatException {
    	return new String(uncompress(Base64.getDecoder().decode(data)),charset);
    }
    
    /**
     * 执行数据解压处理(解压格式:GZIP/ZLIB)
     * @param data 被解压的数据
     * @return 解压后的数据
     * @throws IOException
     * @throws DataFormatException 
     */
    public static byte[] uncompress(byte[] data) throws IOException, DataFormatException {
    	return uncompress(data,true);
    }
    
    /**
     * 执行数据解压处理(解压格式:GZIP/ZLIB)
     * @param data 被解压的数据
     * @param nowrap 是否解包压缩头(如头域:checksum)
     * @return 解压后的数据
     * @throws IOException
     * @throws DataFormatException 
     */
    public static byte[] uncompress(byte[] data, Boolean nowrap) throws IOException, DataFormatException {
    	if(null==nowrap) nowrap=true;
        Inflater inflater = new Inflater(nowrap);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        
        inflater.setInput(data);
        byte[] buff = new byte[1024];
        try {
            while (!inflater.finished()) baos.write(buff, 0, inflater.inflate(buff));
            return baos.toByteArray();
        } finally {
            baos.close();
            inflater.end();
        }
    }
}
