package com.whut.database.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/*
    通过套接字传输数据
 */
public class Transporter {

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    /*
        发送数据
     */
    public void send(byte[] data) throws Exception{
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    /*
        接收数据
     */
    public byte[] receive() throws Exception{
        String line = reader.readLine();
        if (line == null) close();
        return hexDecode(line);
    }

    public void close() throws Exception{
        socket.close();
        reader.close();
        writer.close();
    }

    /*
        字节数组转16进制字符串，进行了加密处理
     */
    private String hexEncode(byte[] data){
        return Hex.encodeHexString(data,true) + "\n";
    }

    /*
        16进制字符串转字节数组，进行了解密处理
     */
    private byte[] hexDecode(String data) throws DecoderException {
        return Hex.decodeHex(data);
    }


}
