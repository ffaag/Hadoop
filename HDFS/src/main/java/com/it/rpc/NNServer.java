package com.it.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author ZuYingFang
 * @time 2021-12-04 10:27
 * @description 实现通信接口
 */
public class NNServer implements RPCProtocol{

    public static void main(String[] args) throws IOException {
        // 启动服务
        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer())
                .build();


        System.out.println("服务器开始工作");
        server.start();
    }

    @Override
    public void mkDirs(String path) {
        System.out.println("服务器收到了客户端的请求" + path);
    }
}
