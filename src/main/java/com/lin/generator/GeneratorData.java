package com.lin.generator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author lzr
 * @date 2020-09-20 19:46:21
 * 自动生成数据保存到文件中
 */
public class GeneratorData {
    public static void main(String[] args) {
        File file = new File("uerBehavior.txt");
        if(!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 准备长度是2的字节数组，用88,89初始化，其对应的字符分别是X,Y
        byte[] data = {88,89};
        // 创建基于文件的输出流
        FileWriter fw = null;
        try {
            fw = new FileWriter(file);
            fw.write("");
            for(int i = 0; i < 1000; i++){
                StringBuffer str = new StringBuffer();
                str.append(100).append(",").append(10000).append(",").append(1).append(",").append("pv").append(",")
                        .append(System.currentTimeMillis()).append("\n");
                // 把数据写入到输出流
                fw.write(String.valueOf(str));
            }

            // 关闭输出流
            fw.flush();
            fw.close();
            System.out.println("=======写入文件成功=========");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
