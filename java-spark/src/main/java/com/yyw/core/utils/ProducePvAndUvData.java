package com.yyw.core.utils;

import java.io.*;
import java.util.Date;
import java.util.Random;

/**
 * 模拟生成网站访问数据
 */

public class ProducePvAndUvData {
    //ip
    public static Integer IP = 223;
    //区域
    public static String[] ADDRESS = {
            "北京", "天津", "上海", "重庆", "河北", "辽宁","山西",
            "吉林", "江苏", "浙江", "黑龙江", "安徽", "福建", "江西",
            "山东", "河南", "湖北", "湖南", "广东", "海南", "四川",
            "贵州", "云南", "山西", "甘肃", "青海", "台湾", "内蒙",
            "广西", "西藏", "宁夏", "新疆", "香港", "澳门"
    };
    //日期
    public static String DATE = "2020-03-19";
    //时间戳
    public static Long TIMESTAMP = 0L;
    //用户ID
    public static Long USERID = 0L;
    //网站
    public static String[] WEBSITE = {"www.baidu.com", "www.taobao.com", "www.dangdang.com","www.jd.com", "www.suning.com","www.mi.com","www.huawei.com"};
    //操作
    public static String[] ACTION = {"注册","评论","浏览","登录","购买","点击","登出"};

    /**
     * 创建文件
     */
    public static Boolean createFile(String pathFileName){
        File file = new File(pathFileName);
        try {
            if (file.exists()) {
                file.delete();
            } else {
                return file.createNewFile();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 向文件写入数据
     */
    public static void WriteDataToFile(String pathFileName, String content){
        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        PrintWriter pw = null;
        try{
            //产生一行模拟数据
            File file = new File(pathFileName);
            fos = new FileOutputStream(file, true);
            osw = new OutputStreamWriter(fos, "UTF-8");
            pw = new PrintWriter(osw);
            pw.write(content+"\n");

            //关闭文件操作流
            pw.close();
            osw.close();
            fos.close();

        } catch (IOException e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        String pathFileName = "./java-spark/data/pvuvdata";
        //创建文件
        boolean createFile = createFile(pathFileName);
        if(createFile){
            int i = 0;
            //产生3万条数据
            while(i<80000){

                Random random = new Random();
                //模拟一个IP
                String ip = random.nextInt(IP) + "." + random.nextInt(IP) + "." +random.nextInt(IP) + "." +random.nextInt(IP);
                //模拟地址
                String address = ADDRESS[random.nextInt(34)];
                //模拟日期
                String date = DATE;
                //模拟用户ID
                Long userId = Math.abs(random.nextLong());

                /**
                 * 这里的while模拟是同一个用户不同时间点对不同网站的操作
                 */
                int j = 0;
                Long timestamp = 0L;
                String website = "未知网站";
                String action = "未知行为";
                int flag = random.nextInt(5)|1;
                while(j<flag){
                    //模拟timestamp
                    timestamp = new Date().getTime();
                    //模拟网站
                    website = WEBSITE[random.nextInt(7)];
                    //模拟行为
                    action = ACTION[random.nextInt(6)];
                    j++;

                    // 拼装
                    String content = ip+"\t"+address+"\t"+date+"\t"+timestamp+"\t"+website+"\t"+action;
                    //向文件中写入数据
                    WriteDataToFile(pathFileName, content);

                }
                i++;

            }
        }

    }


}
