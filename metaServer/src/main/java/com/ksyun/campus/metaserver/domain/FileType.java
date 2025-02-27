package com.ksyun.campus.metaserver.domain;

public enum FileType
{
    //区分不同类型的文件或目录
    Unknown(0),  Volume(1),  File(2),  Directory(3);

    private int code;
    FileType(int code) {
        this.code=code;
    }
    public int getCode(){
        return code;
    }
    public static FileType get(int code){
        switch (code){
            case 1:
                return Volume;
            case 2:
                return File;
            case 3:
                return Directory;
            default:
                return Unknown;
        }
    }
}
