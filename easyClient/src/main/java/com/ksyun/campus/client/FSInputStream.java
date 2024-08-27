package com.ksyun.campus.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.client.util.ReadResponse;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.Buffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class FSInputStream extends InputStream {

    //数据块大小为64K
    private static final int BUFFER_SIZE = 64 * 1024;

    private final CloseableHttpClient httpClient;

    public FSInputStream(List<String> ipAddress, List<Integer> port, String fileSystemName, String path) {
        httpClient = HttpClients.createDefault();
        buffer = new byte[BUFFER_SIZE];
        Path = path;
        offset = 0;
        FileSystemName = fileSystemName;
        bufferPos = 0;

        //初始化数据服务器部分
        DataServerUrlList = new ArrayList<String>();
        for(int i = 0;i < ipAddress.size();i++){
            DataServerUrlList.add("http://" + ipAddress.get(i) + ":" + port.get(i) + "/read");
        }
    }

    //填充
    private int fillBuffer() throws IOException {

        for (int i = 0; i < DataServerUrlList.size(); i++) {
            String requestUrl = DataServerUrlList.get(i) + "?path=" + URLEncoder.encode(Path, StandardCharsets.UTF_8.name());
            requestUrl += "&offset=" + offset;
            requestUrl += "&length=" + BUFFER_SIZE;

            //创建Get请求
            HttpGet httpGet = new HttpGet(requestUrl);
            httpGet.addHeader("fileSystemName", FileSystemName);

            try(CloseableHttpResponse response = httpClient.execute(httpGet)){
                if(response.getCode() == 200){
                    HttpEntity entity = response.getEntity();
                    if(entity != null){
                        // 将返回体转换为字符串
                        String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);

                        if (responseBody == null || responseBody.trim().isEmpty()) {
                            // 处理空响应体的情况
                            bufferPos = -1;
                            return -1;
                        }

                        // 使用 Jackson ObjectMapper 反序列化 JSON 字符串为 ReadResponse 对象

                        ObjectMapper objectMapper = new ObjectMapper();
                        ReadResponse readResponse = objectMapper.readValue(responseBody, ReadResponse.class);

                        // 处理 ReadResponse 对象
                        byte[] entityContent = readResponse.getContent();
                        int size = readResponse.getSize();

                        int bytesRead = entityContent.length;

                        if(size != bytesRead){
                            bytesRead = size;
                        }

                        if(bytesRead > 0){
                            // 将数据填充到buffer中
                            System.arraycopy(entityContent, 0, buffer, 0, bytesRead);
                            bufferPos = offset % BUFFER_SIZE; // 更新缓冲区指针

                            //填充成功，直接返回
                            return bytesRead;
                        }
                    }else{
                        bufferPos = -1;
                        return -1;
                    }
                }else{
                    //尝试下一个DataServer
                    continue;
                }
            }catch (Exception e){
                //尝试下一个DataServer
                e.printStackTrace();
                continue;
            }
        }
        throw new IOException("无法从任何一个DataServer中获得填充数据");
    }

    //数据服务器地址列表
    private List<String> DataServerUrlList;

    //操作的文件所在命名空间
    private String FileSystemName;

    //文件绝对路径
    private String Path;

    //偏移量记录
    private int offset;

    //缓冲区
    private byte[] buffer;

    //缓冲区指标
    private int bufferPos;

    private int capacity = BUFFER_SIZE;

    @Override
    public int read() throws IOException {

        if(offset == 0){
            capacity = fillBuffer();
        }

        //更新缓冲区
        if(bufferPos == BUFFER_SIZE){
            capacity = fillBuffer();
        }

        if(capacity != BUFFER_SIZE && bufferPos == capacity){
            return -1;
        }

        //代表已经全部读完
        if (bufferPos == -1) {
            return -1;
        }

        // 从缓冲区读取一个字节
        int result = buffer[0] & 0xFF;

        // 更新偏移量
        offset++;
        bufferPos++;

        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inread(b, offset, len, off);
    }

    public int inread(byte[] b, int off, int len, int bpos) throws IOException {

        int blockInOffset = off % BUFFER_SIZE;

        //需要读取多少次
        int loopT = -1;

        int tmp = len - BUFFER_SIZE + blockInOffset;

        if(tmp <= 0){
            loopT = 1;
        }else{
            loopT = tmp / BUFFER_SIZE + 2;
        }

        if(capacity != BUFFER_SIZE){
            loopT = 1;
        }

        int byteread = 0;

        offset = off;

        if(capacity != BUFFER_SIZE && bufferPos == capacity){
            return -1;
        }

        //循环读取
        for(int i = 0; i < loopT; i++){
            capacity = fillBuffer();
            if(bufferPos == -1){
                return -1;
            }

            if(i != loopT - 1) {
                System.arraycopy(buffer, bufferPos, b, byteread + bpos, BUFFER_SIZE - bufferPos);
                byteread += BUFFER_SIZE - bufferPos;
                offset += BUFFER_SIZE - bufferPos;
                bufferPos += BUFFER_SIZE - bufferPos;
            }else{
                int tmplength = Math.min(len - byteread, capacity - bufferPos);
                System.arraycopy(buffer, bufferPos, b, byteread + bpos, tmplength);
                bufferPos += tmplength;
                offset += tmplength;
                byteread += tmplength;
            }
        }

        return byteread;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
        super.close();
    }
}
