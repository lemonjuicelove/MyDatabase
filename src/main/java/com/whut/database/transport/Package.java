package com.whut.database.transport;

/*
    服务端客户端之间的数据传输结构
 */
public class Package {

    byte[] data;
    Exception err;

    public Package(byte[] data, Exception er) {
        this.data = data;
        this.err = er;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
