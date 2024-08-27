package com.ksyun.campus.client;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FSOutputStream extends OutputStream {

    //数据块大小为64K
    private static final int BUFFER_SIZE = 64 * 1024;

    private final CloseableHttpClient httpClient;

    //缓冲区
    private byte[] buffer;

    //缓冲区指标
    private int bufferPos;

    //操作的文件所在命名空间
    private String FileSystemName;

    //数据服务器地址列表
    private List<String> DataServerUrlList;

    //文件绝对路径
    private String Path;

    //偏移量记录
    private long offset;

    private void flushBuffer() throws IOException {
        try {
            if (bufferPos > 0) {
                for (int i = 0;i < DataServerUrlList.size();i++) {
                    //附加参数
                    String requestUrl = DataServerUrlList.get(i) + "?path=" + URLEncoder.encode(Path, StandardCharsets.UTF_8.name());
                    requestUrl += "&offset=" + offset;
                    requestUrl += "&length=" + bufferPos;

                    //创建Post请求,附带缓冲区数据
                    HttpPost post = new HttpPost(requestUrl);

                    post.addHeader("fileSystemName", FileSystemName);
                    post.setEntity(new ByteArrayEntity(buffer, 0, bufferPos, ContentType.APPLICATION_OCTET_STREAM));

                    //保证必须发送成功--暂时这么写
                    while (true) {
                        CloseableHttpResponse response = httpClient.execute(post);
                        int statusCode = response.getCode();
                        if (statusCode != 200) {
                            System.out.println("发送package" + offset + "失败");
                        } else {
                            System.out.println("发送package" + offset + "成功");
                            break;
                        }
                    }
                }
            }
        }finally {
            if(bufferPos > 0) {
                offset += bufferPos;
                bufferPos = 0;
            }
        }
    }

    //构造函数
    public FSOutputStream(List<String> ipAddress, List<Integer> port, String fileSystemName, String path) {
        //初始化公共部分
        httpClient = HttpClients.createDefault();
        buffer = new byte[BUFFER_SIZE];
        Path = path;
        offset = 0;
        FileSystemName = fileSystemName;

        //初始化数据服务器部分
        DataServerUrlList = new ArrayList<String>();
        for(int i = 0;i < ipAddress.size();i++){
            DataServerUrlList.add("http://" + ipAddress.get(i) + ":" + port.get(i) + "/write");
        }
    }

    @Override
    public void write(int b) throws IOException {
        if(bufferPos >= BUFFER_SIZE){
            flushBuffer();
        }
        buffer[bufferPos++] = (byte) b;
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int tmpLen = len;
        while(tmpLen > 0){
            int spaceInBuffer = BUFFER_SIZE - bufferPos;
            int bytesToCopy = Math.min(spaceInBuffer, tmpLen);

            //复制数据
            System.arraycopy(b, off, buffer, bufferPos, bytesToCopy);
            bufferPos += bytesToCopy;
            off += bytesToCopy;
            tmpLen -= bytesToCopy;

            //刷新缓冲区
            if (bufferPos >= BUFFER_SIZE) {
                flushBuffer();
            }
        }
    }

    @Override
    public void close() throws IOException {
        flushBuffer();
        httpClient.close();
        super.close();
    }
}
