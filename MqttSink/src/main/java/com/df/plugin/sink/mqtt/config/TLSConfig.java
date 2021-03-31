package com.df.plugin.sink.mqtt.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.codec.binary.Base64;

/**
 * @author Lixiang
 * @description 原创通用SSL(TLS)工具
 */
public class TLSConfig {
	/**
	 * 获取单向认证工厂
	 * @param rootCaFile 根证书文件
	 * @return SSLSocketFactory
	 * @throws Exception
	 */
	public static final SSLSocketFactory getSSLSocketFactory(File rootCaFile) throws Exception {
		//读取根证书文件内容生成根证书Certificate
		Certificate rootCa=null;
    	InputStream rootCaFis=null;
		try{
			rootCaFis = Files.newInputStream(rootCaFile.toPath());
			rootCa=CertificateFactory.getInstance("X.509").generateCertificate(rootCaFis);
		}finally{
			if(null!=rootCaFis) rootCaFis.close();
    	}
		
		//将根证书Certificate放置到根键存储KeyStore
        KeyStore rootCaKs = KeyStore.getInstance("JKS");
        rootCaKs.load(null, null);
        rootCaKs.setCertificateEntry("ca-certificate", rootCa);
        
        //使用根键存储KeyStore初始化受信管理工厂TrustManagerFactory
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(rootCaKs);
        
        //使用受信管理工厂初始化SSL上下文并返回Socket工厂
        SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(null, tmf.getTrustManagers(), new SecureRandom());
        return context.getSocketFactory();
    }
	
	/**
	 * 获取双向认证工厂
	 * @param rootCaFile 根证书文件
	 * @param clientCaFile 客户端生成的证书文件
	 * @param clientKeyFile 客户端生成的秘钥文件
	 * @param clientCaPassword 客户端生成证书时输入的密码
	 * @return SSLSocketFactory
	 */
	public static final SSLSocketFactory getSSLSocketFactory(File rootCaFile, File clientCaFile, File clientKeyFile, String clientCaPassword) throws Exception {
		//读取根证书文件内容生成根证书Certificate
		Certificate rootCa=null;
    	InputStream rootCaFis=null;
    	try{
    		rootCaFis=Files.newInputStream(rootCaFile.toPath());
    		rootCa=CertificateFactory.getInstance("X.509").generateCertificate(rootCaFis);
    	}finally{
    		if(null!=rootCaFis) rootCaFis.close();
    	}
		
    	//将根证书Certificate放置到根键存储KeyStore
    	KeyStore rootCaKs=KeyStore.getInstance("JKS");
		rootCaKs.load(null, null);
		rootCaKs.setCertificateEntry("ca-certificate", rootCa);
    	
    	//使用根键存储KeyStore初始化受信管理工厂TrustManagerFactory
    	TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(rootCaKs);
       
        //读取客户端证书文件内容生成客户端证书Certificate
        Certificate clientCa=null;
        InputStream clientCaFis=null;
        try{
        	clientCaFis=Files.newInputStream(clientCaFile.toPath());
        	clientCa=CertificateFactory.getInstance("X.509").generateCertificate(clientCaFis);
        }finally{
        	if(null!=clientCaFis) clientCaFis.close();
    	}
        
        //将客户端证书Certificate放置到客户端键存储KeyStore
        char[] clientCaPwds=clientCaPassword.toCharArray();
        KeyStore clientCaKs=KeyStore.getInstance(KeyStore.getDefaultType());
    	clientCaKs.load(null, null);
    	clientCaKs.setCertificateEntry("certificate", clientCa);
    	clientCaKs.setKeyEntry("private-key", getPrivateKey(clientKeyFile), clientCaPwds,new java.security.cert.Certificate[]{clientCa});
        
        //使用客户端键存储KeyStore初始化客户端键管理工厂
        KeyManagerFactory kmf=KeyManagerFactory.getInstance("PKIX");
        kmf.init(clientCaKs, clientCaPwds);
        
        //使用客户端键管理工厂和受信管理工厂初始化SSL上下文并返回Socket工厂
        SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
        return context.getSocketFactory();
    }

    /**
     * 将秘钥字串通过PKCS8编码生成RSA秘钥对象
     * @param keyFile 秘钥文件
     * @return 秘钥对象
     * @throws Exception
     */
    public static RSAPrivateKey getPrivateKey(File keyFile) throws Exception {
    	 String line=null;
    	 BufferedReader bufferedReader=null;
         StringBuilder builder=new StringBuilder();
         try{
        	 bufferedReader=Files.newBufferedReader(keyFile.toPath());
             while ((line = bufferedReader.readLine()) != null) {
                 if (line.charAt(0) == '-') continue;
                 builder.append(line).append("\r");
             }
         }finally{
        	 if(null!=bufferedReader) bufferedReader.close();
         }
         
        byte[] buffer = new Base64().decode(builder.toString());
        return (RSAPrivateKey) KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(buffer));
    }
}
