package com.whut.database.client;

import java.util.Scanner;

public class Shell {

    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    /*
        读输入，处理输入
     */
    public void run(){
        Scanner sc = new Scanner(System.in);
        try {
            while (true){
                System.out.println(":> ");
                String sqlStatement = sc.nextLine();
                if("exit".equals(sqlStatement) || "quit".equals(sqlStatement)) break;

                try {
                    byte[] res = client.execute(sqlStatement.getBytes());
                    System.out.println(new String(res));
                }catch (Exception e){
                    System.out.println(e.getMessage());
                }
            }
        }finally {
            sc.close();
            client.close();
        }
    }
}
