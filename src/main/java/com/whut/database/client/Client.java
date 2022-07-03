package com.whut.database.client;

import com.whut.database.transport.Package;
import com.whut.database.transport.Packager;

/*
    客户端
 */
public class Client {

    private RoundTripper rt;

    public Client(Packager packager){
        this.rt = new RoundTripper(packager);
    }

    /*
        处理SQL语句，并返回结果
     */
    public byte[] execute(byte[] statement) throws Exception {
        Package pkg = new Package(statement,null);
        Package resPkg = rt.roundTrip(pkg);
        if (resPkg.getErr() != null) throw resPkg.getErr();

        return resPkg.getData();
    }

    public void close(){
        try {
            rt.close();
        } catch (Exception e) {

        }
    }


}
