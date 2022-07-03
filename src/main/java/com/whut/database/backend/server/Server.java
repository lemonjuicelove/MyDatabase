package com.whut.database.backend.server;

import com.whut.database.backend.Tbm.TableManager;
import com.whut.database.transport.Encoder;
import com.whut.database.transport.Package;
import com.whut.database.transport.Packager;
import com.whut.database.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
    服务端
 */
public class Server {

    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start(){
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port：" + port);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(10,20,1L, TimeUnit.SECONDS,new ArrayBlockingQueue<>(100),new ThreadPoolExecutor.CallerRunsPolicy());

        // 将服务端连接交给线程池进行处理
        try {
            while (true){
                Socket socket = ss.accept();
                HandleSocket task = new HandleSocket(socket, tbm);
                pool.execute(task);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                ss.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

/*
    循环处理客户端的请求
 */
class HandleSocket implements Runnable{

    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        System.out.println("Establish connection：" + address.getAddress().getHostAddress() + ":" + address.getPort());

        Packager packager = null;
        try{
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t,e);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        Executor executor = new Executor(tbm);
        while (true){
            // 接收客户端的数据
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch (Exception e) {
                break;
            }
            byte[] sql = pkg.getData();

            // 处理SQL语句
            byte[] res = null;
            Exception e = null;
            try {
                res = executor.execute(sql);
            } catch (Exception ex) {
                e = ex;
                e.printStackTrace();
            }

            // 向客户端返回结果
            pkg = new Package(res,e);
            try {
                packager.send(pkg);
            } catch (Exception ex) {
                ex.printStackTrace();
                break;
            }
        }

        executor.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
