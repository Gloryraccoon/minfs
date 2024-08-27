package test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSInputStream;
import com.ksyun.campus.client.FSOutputStream;
import com.ksyun.campus.client.domain.*;
import org.springframework.http.converter.json.GsonBuilderUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Scanner;

public class TestClient {

    public static int BUFFER_SIZE = 1024;

    public static boolean upload(String filename, String path, String fileSystemName){
        EFileSystem fs = new EFileSystem(fileSystemName);

        byte[] buffer = new byte[BUFFER_SIZE];

        FSOutputStream fsOutputStream = fs.create(path);

        try(FileInputStream fileStream = new FileInputStream(filename)){
            int bytesRead = 0;
            while((bytesRead = fileStream.read(buffer)) != -1){
                fsOutputStream.write(buffer, 0, bytesRead);
            }
            fsOutputStream.close();
            System.out.println("上传成功");

            return true;
        }catch(Exception e){
            System.out.printf("上传失败,发生异常");
            return false;
        }
    }

    public static boolean download(String output, String path, String fileSystemName){
        EFileSystem fs = new EFileSystem(fileSystemName);

        byte[] buffer = new byte[BUFFER_SIZE];

        FSInputStream fsInputStream = fs.open(path);
        if(fsInputStream == null){
            System.out.println("无法下载文件-可能不存在");
            return false;
        }

        File rootDir = new File(System.getProperty("user.dir"));
        File outputFile = new File(rootDir, output);

        try(FileOutputStream fileOutputStream = new FileOutputStream(outputFile)){
            int bytesRead;

            while ((bytesRead = fsInputStream.read(buffer, 0, buffer.length)) != -1) {
                fileOutputStream.write(buffer, 0, bytesRead);
            }

            System.out.println("下载文件成功");
            return true;

        }catch(Exception e){
            System.out.println("下载文件发生异常");
            return false;
        }
    }

    public static void deleteFileOrDir(String path, String fileSystemName){
        EFileSystem fs = new EFileSystem(fileSystemName);
        fs.delete(path);
    }

    public static void showFileMeta(StatInfo info){
        System.out.println("path= " + info.getPath());
        System.out.println("mtime= " + info.getMtime());
        System.out.println("size= " + info.getSize());
        System.out.println("FileType= " + info.getType());

        if(info.getReplicaData() == null){
            return;
        }

        for(int i = 0;i < info.getReplicaData().size();i++){
            ReplicaData replicaData = info.getReplicaData().get(i);
            System.out.println("ReplicaData " + i);
            System.out.println("id=" + replicaData.id);
            System.out.println("dsNode=" + replicaData.dsNode);
            System.out.println("path=" + replicaData.path);
        }
    }

    public static void getFileMeta(String path, String fileSystemName){
        EFileSystem fs = new EFileSystem(fileSystemName);
        StatInfo info =  fs.getFileStats(path);
        if(info == null){
            System.out.println("元数据不存在");
            return;
        }

        showFileMeta(info);
    }

    public static void getListFileMeta(String path, String fileSystemName){
        EFileSystem fs = new EFileSystem(fileSystemName);
        List<StatInfo> statlist = fs.listFileStats(path);
        if(statlist == null){
            System.out.println("元数据不存在");
            return;
        }

        for(int i = 0;i < statlist.size();i++){
            showFileMeta(statlist.get(i));
        }
    }

    public static void mkdiretory(String path, String fileSystemName){
        EFileSystem fs = new EFileSystem(fileSystemName);
        if(fs.mkdir(path)){
            System.out.println("目录创建成功");
        }else{
            System.out.println("目录创建失败");
        }
    }

    public static void getClusterInfo(){
        EFileSystem fs = new EFileSystem();
        ClusterInfo clusterInfo = fs.getClusterInfo();

        MetaServerMsg mastermetaServerMsg = clusterInfo.getMasterMetaServer();
        System.out.println("------------------Master-MetaServer------------------");
        System.out.println(mastermetaServerMsg.getHost() + ":" + mastermetaServerMsg.getPort());
        List<MetaServerMsg> slavemetaServerMsgList = clusterInfo.getSlaveMetaServerList();
        System.out.println("------------------Slave-MetaServer-------------------");

        if(slavemetaServerMsgList != null) {
            for (int i = 0; i < slavemetaServerMsgList.size(); i++) {
                MetaServerMsg tmp = slavemetaServerMsgList.get(i);
                System.out.println("slave" + i + " :> " + tmp.getHost() + ":" + tmp.getPort());
            }
        }

        List<DataServerMsg> dataServerList = clusterInfo.getDataServer();

        System.out.println("----------------------DataServer----------------------");
        if(dataServerList != null){
            for (int i = 0; i < dataServerList.size(); i++) {
                DataServerMsg tmp = dataServerList.get(i);
                System.out.println("dataServer" + i + " :> " + tmp.getHost() + ":" + tmp.getPort());
            }
        }
    }

    public static void main(String[] args) {
        boolean exit = false;

        while(!exit){
            Scanner scanner = new Scanner(System.in);
            System.out.println("---------------------------分布式文件存储系统---------------------------");
            System.out.println("0.退出");
            System.out.println("1.上传文件");
            System.out.println("2.下载文件");
            System.out.println("3.删除文件");
            System.out.println("4.查询文件信息");
            System.out.println("5.查询整个目录子文件元数据");
            System.out.println("6.创建目录");
            System.out.println("7.查询集群信息");
            System.out.println("-------------------------------刘梓杰----------------------------------");
            System.out.print("选择>：");
            int choice = 0;

            try {
                choice = Integer.parseInt(scanner.nextLine()); // 使用 nextLine 然后解析整数
            } catch (NumberFormatException e) {
                System.out.println("请输入有效的数字");
                continue; // 如果输入无效，重新显示菜单
            }

            String fileSystemName;
            String filename;
            String path;

            switch(choice){
                case 0:
                    exit = true;
                    break;
                case 1:
                    System.out.println("输入用户名:> ");
                    fileSystemName = scanner.nextLine();
                    System.out.println("输入待上传文件名:> ");
                    filename = scanner.nextLine();
                    System.out.println("输入存储路径:> ");
                    path = scanner.nextLine();
                    upload(filename, path, fileSystemName);
                    break;
                case 2:
                    System.out.println("输入用户名:> ");
                    fileSystemName = scanner.nextLine();
                    System.out.println("输入存储路径:> ");
                    path = scanner.nextLine();
                    System.out.println("输入文件输出名字:> ");
                    filename = scanner.nextLine();
                    download(filename, path, fileSystemName);
                    break;
                case 3:
                    System.out.println("输入用户名:> ");
                    fileSystemName = scanner.nextLine();
                    System.out.println("输入存储路径:> ");
                    path = scanner.nextLine();
                    deleteFileOrDir(path, fileSystemName);
                    break;
                case 4:
                    System.out.println("输入用户名:> ");
                    fileSystemName = scanner.nextLine();
                    System.out.println("输入存储路径:> ");
                    path = scanner.nextLine();
                    getFileMeta(path, fileSystemName);
                    break;
                case 5:
                    System.out.println("输入用户名:> ");
                    fileSystemName = scanner.nextLine();
                    System.out.println("输入目录路径:> ");
                    path = scanner.nextLine();
                    getListFileMeta(path, fileSystemName);
                    break;
                case 6:
                    System.out.println("输入用户名:> ");
                    fileSystemName = scanner.nextLine();
                    System.out.println("输入目录路径:> ");
                    path = scanner.nextLine();
                    mkdiretory(path, fileSystemName);
                    break;
                case 7:
                    getClusterInfo();
                    break;
                default:
                    System.out.println("未知选择");
                    break;
            }
        }
    }
}
