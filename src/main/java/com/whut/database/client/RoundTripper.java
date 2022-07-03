package com.whut.database.client;

import com.whut.database.transport.Package;
import com.whut.database.transport.Packager;

public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        // 发送客户端SQL语句
        packager.send(pkg);

        // 返回服务端的数据
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }

}
