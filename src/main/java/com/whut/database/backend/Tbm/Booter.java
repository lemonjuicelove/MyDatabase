package com.whut.database.backend.Tbm;

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import com.whut.database.backend.utils.Panic;
import com.whut.database.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/*
    保存第一个表的uid
    所有的表按链表的方式连接
 */
public class Booter {

    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    public Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    /*
        删除临时的表信息文件
     */
    private static void removeBadTmp(String path){
        File file = new File(path + BOOTER_TMP_SUFFIX);
        file.delete();
    }

    /*
        创建Booter
     */
    public static Booter create(String path){
        removeBadTmp(path);
        File file = new File(path + BOOTER_SUFFIX);
        try {
            if (!file.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (!file.canRead() || !file.canWrite()) Panic.panic(Error.FileCannotRWException);

        return new Booter(path,file);
    }

    /*
        打开Booter
     */
    public static Booter open(String path){
        removeBadTmp(path);
        File file = new File(path + BOOTER_SUFFIX);
        if (!file.exists()) Panic.panic(Error.FileNotExistsException);
        if (!file.canRead() || !file.canWrite()) Panic.panic(Error.FileCannotRWException);

        return new Booter(path,file);
    }

    /*
        加载表信息
     */
    public byte[] load(){
        byte[] buf = null;
        try{
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf;
    }

    /*
        更新表信息
     */
    public void update(byte[] data){
        File tmpFile = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmpFile.createNewFile();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!tmpFile.canRead() || !tmpFile.canWrite()) Panic.panic(Error.FileCannotRWException);

        // 将数据写到临时文件中
        try(FileOutputStream out = new FileOutputStream(tmpFile)){
            out.write(data);
            out.flush();
        } catch (Exception e) {
            Panic.panic(e);
        }

        // 通过操作系统重命名进行替换，保证原子性
        try {
            Files.move(tmpFile.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Panic.panic(e);
        }

        file = new File(path + BOOTER_SUFFIX);
        if (!file.canRead() || !file.canWrite()) Panic.panic(Error.FileCannotRWException);
    }


}
