package com.whut.database.backend;

import com.whut.database.backend.DM.DataManager;
import com.whut.database.backend.TM.TransactionManager;
import com.whut.database.backend.Tbm.TableManager;
import com.whut.database.backend.VM.VersionManager;
import com.whut.database.backend.VM.VersionManagerImpl;
import com.whut.database.backend.server.Server;
import com.whut.database.backend.utils.Panic;
import com.whut.database.common.Error;
import org.apache.commons.cli.*;

/*
    服务器的启动入口
 */
public class Launcher {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1 << 20) *64;

    public static final long KB = 1 << 10;

    public static final long MB = 1 << 20;

    public static final long GB = 1 << 30;


    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open",true,"-open DBPath");
        options.addOption("create",true,"-create DBPath");
        options.addOption("mem",true,"-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("open")){
            openDB(cmd.getOptionValue("open"),parseMem(cmd.getOptionValue("mem")));
        }else if (cmd.hasOption("create")){
            createDB(cmd.getOptionValue("create"));
        }else{
            System.out.println("Usage：launcher (open|create) DBPath");
        }
    }

    /*
        创建一个数据库
     */
    private static void createDB(String path){
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm,dm);
        TableManager.create(path,vm,dm);
        tm.close();
        dm.close();
    }

    /*
        打开数据库
     */
    private static void openDB(String path, long mem){
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm,dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port,tbm).start();
    }

    /*
        设置数据库内存
     */
    private static long parseMem(String memStr){
        if (memStr == null || "".equals(memStr)) return DEFALUT_MEM;

        if (memStr.length() < 2) Panic.panic(Error.InvalidMemException);

        String unit = memStr.substring(memStr.length()-2);
        long mem = Long.parseLong(memStr.substring(0,memStr.length()-2));
        switch (unit){
            case "KB":
                return mem*KB;
            case "MB":
                return mem*MB;
            case "GB":
                return mem*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }

        return DEFALUT_MEM;
    }


}
