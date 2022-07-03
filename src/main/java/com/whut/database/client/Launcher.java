package com.whut.database.client;

import com.whut.database.transport.Encoder;
import com.whut.database.transport.Packager;
import com.whut.database.transport.Transporter;
import org.checkerframework.checker.units.qual.C;

import java.net.Socket;

/*
    客户端的启动入口
 */
public class Launcher {

    public static void main(String[] args) throws Exception{
        Socket socket = new Socket("127.0.0.1",9999);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter,encoder);
        Client client = new Client(packager);
        Shell shell = new Shell(client);

        shell.run();
    }

}
