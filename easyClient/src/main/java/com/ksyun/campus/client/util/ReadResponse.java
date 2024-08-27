package com.ksyun.campus.client.util;

//关于read接口的返回体类
public class ReadResponse {
    byte[] content;
    int size;

    public ReadResponse() {
    }

    public ReadResponse(byte[] content, int size) {
        this.content = content;
        this.size = size;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
