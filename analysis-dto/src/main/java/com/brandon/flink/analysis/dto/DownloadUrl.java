package com.brandon.flink.analysis.dto;

import sun.misc.BASE64Encoder;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class DownloadUrl {

    public static void main(String[] args) throws Exception {
        String url = "https://6d69-mic-vision-dds6c-1303241806.tcb.qcloud.la/images/1602232189744.png";
        String path = "D:\\opt\\vision\\test2.jpg";
        downloadPic(url, path);

    }

    private static void downloadPic(String url, String path) throws Exception {
        URL u = null;
        try {
            u = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            if (conn instanceof HttpsURLConnection)  {
                SSLContext sc = SSLContext.getInstance("SSL");
                sc.init(null, new TrustManager[]{new TrustAnyTrustManager()}, new java.security.SecureRandom());
                ((HttpsURLConnection) conn).setSSLSocketFactory(sc.getSocketFactory());
                ((HttpsURLConnection) conn).setHostnameVerifier(new TrustAnyHostnameVerifier());
            }
            DataInputStream dataInputStream = new DataInputStream(conn.getInputStream());

            FileOutputStream fileOutputStream = new FileOutputStream(new File(path));
            ByteArrayOutputStream output = new ByteArrayOutputStream();

            byte[] buffer = new byte[1024];
            int length;

            while ((length = dataInputStream.read(buffer)) > 0) {
                output.write(buffer, 0, length);
            }
            BASE64Encoder encoder = new BASE64Encoder();
            String encode = encoder.encode(buffer);//返回Base64编码过的字节数组字符串
//            System.out.println(encode);
            fileOutputStream.write(output.toByteArray());
            dataInputStream.close();
            fileOutputStream.close();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class TrustAnyTrustManager implements X509TrustManager {

        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[]{};
        }
    }

    private static class TrustAnyHostnameVerifier implements HostnameVerifier {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }


    }
}



