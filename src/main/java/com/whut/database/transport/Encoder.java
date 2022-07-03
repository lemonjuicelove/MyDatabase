package com.whut.database.transport;

import com.google.common.primitives.Bytes;
import com.whut.database.common.Error;

import java.util.Arrays;

/*
    数据的编码方式
        [Flag][data]
        Flag为1，发送的是错误，Flag为0，发送的是数据
 */
public class Encoder {

    /*
        发送前编码
     */
    public byte[] encode(Package pkg){
        if (pkg.getErr() != null){
            Exception err = pkg.getErr();
            String msg = "Intern server error";
            if (err.getMessage() != null) msg = err.getMessage();

            return Bytes.concat(new byte[]{1},msg.getBytes());
        }else{
            return Bytes.concat(new byte[]{0},pkg.getData());
        }
    }

    /*
        接收后解码
     */
    public Package decode(byte[] data) throws Exception{
        if (data.length < 1) throw Error.InvalidPkgDataException;

        if (data[0] == 0){
            return new Package(Arrays.copyOfRange(data,1,data.length),null);
        }else if(data[0] == 1){
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data,1,data.length))));
        }else{
            throw Error.InvalidPkgDataException;
        }
    }

}
