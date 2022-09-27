package com.it.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author ZuYingFang
 * @time 2021-11-11 19:43
 * @description 客户端代码常用套路
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * 4、不是很重要，用的不多
 * HDFS、zookeeper
 */
public class HdfsClient {

    private FileSystem fileSystem;

    /**
     * 参数优先级
     * （1）客户端代码中设置的值 >     直接在代码中的configuration.set方法中设置的键值对
     * （2）ClassPath下的用户自定义配置文件 >   我们在java的resource下编写的配置文件
     * （3）然后是服务器的自定义配置（xxx-site.xml）>  我们自己在linux中编写的配置文件
     * （4）服务器的默认配置（xxx-default.xml）
     */


    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群的地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");  // 这里是上面的代码优先级的第一个
        // 用户
        String user = "xiaofang";

        // 1、获取到客户端对象
        fileSystem = FileSystem.get(uri, configuration, user);
    }


    // 创建目录
    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {

        // 2、创建一个文件夹
        fileSystem.mkdirs(new Path("/xiyou/huaguoshan"));

    }


    // 上传
    @Test
    public void testPut() throws IOException {

        /**
         * 参数1：是否删除原数据
         * 参数2：是否允许覆盖
         * 参数3：源路径
         * 参数4：目标地址路径
         */
        fileSystem.copyFromLocalFile(false, true, new Path("D:\\sunwukong.txt"), new Path("/xiyou/huaguoshan"));
    }

    // 下载
    @Test
    public void testGet() throws IOException {
        // 最后一个参数是是否开启本地校验，true表示不校验，一般不用，不用管
        fileSystem.copyToLocalFile(false, new Path("/xiyou/huaguoshan"), new Path("E:\\"), true);
    }

    // 删除
    @Test
    public void testRm() throws IOException {
        // 第二个参数是是否递归删除
        fileSystem.delete(new Path("/input"), true);
    }

    // 文件更名和移动
    @Test
    public void testRename() throws IOException {
        fileSystem.rename(new Path("/xiyou/tmp"), new Path("/xiyou/tmp2"));
    }


    // 获取文件详细信息
    @Test
    public void testListFiles() throws IOException {

        // 第二个参数表示是否递归
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/xiyou"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }

    }


    // 判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录：" + fileStatus.getPath().getName());
            }
        }
    }




    @After
    public void close() throws IOException {
        // 3、关闭资源
        fileSystem.close();
    }
}
