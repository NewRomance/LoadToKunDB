package io.transwarp.loadToKunDB;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ReaderTest {
    volatile static byte[] lineBuf;

    public static void main(String[] args) throws IOException {
//        BufferedReader bufferedReader = new BufferedReader(
//                new InputStreamReader(new FileInputStream(args[0]), StandardCharsets.UTF_8));
//        String line;
//        int count = 0;
//        long start = System.currentTimeMillis();
//        while((line=bufferedReader.readLine())!=null){
//            count ++;
//          //  line;
//
//        }
//        long took = System.currentTimeMillis() - start;
//        System.out.println(count + " lines / " + took + " ms = " + 1000*count/took + " lines/s");

      //  BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(args[0]));
        InputStream inputStream = new FileInputStream(args[0]);
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(args[1]));
        int count = 0;
        int n;
        long start = System.currentTimeMillis();
        byte[] buf = new byte[65536];
        lineBuf = new byte[65536];
      //  ArrayList<byte[]> arrayList = new ArrayList<>();
        int cur = 0;
        while((n = inputStream.read(buf)) != -1){
            for (int i=0; i<n; ++i) {
                lineBuf[cur] = buf[i];
                if (lineBuf[cur++] == '\n') {
              //      arrayList.add(lineBuf);
              //      lineBuf = new byte[2048];
                    count ++;
                    byte[] writeBuf = new byte[cur];

                    System.arraycopy(lineBuf,0,writeBuf,0,cur);
                    outputStream.write(writeBuf);
                  //  System.out.println(".");

                    cur = 0;
                }
            }
          //  buf = new byte[32768];
        }
      //  System.out.println(arrayList.size());
        outputStream.close();
        long took = System.currentTimeMillis() - start;
        System.out.println(count + " lines / " + took + " ms = " + 1000*count/took + " lines/s");
    }
}
