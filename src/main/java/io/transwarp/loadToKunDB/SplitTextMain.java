package io.transwarp.loadToKunDB;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

public class SplitTextMain {
    private static String filePath;
    private static int totalShardNum;
    private static int totalColumnNum;
    private static int shardKeyIndex;
    private static String encoding;
    private static String separator;
    private static String enclosedChar;
    private static String outputDir;
    private static String failedFileName;
    private static ShardEvaluater shardEvaluater;

    private static void loadConfig(String initFilePath){
        /**读取配置文件
         * 配置文件路径为英文
         * */
        InputStream in = null;
        Properties p = new Properties();
        try {
            in = new BufferedInputStream(new FileInputStream(initFilePath));
            p.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        filePath= p.getProperty("params.filePath");
        totalShardNum= Integer.valueOf(p.getProperty("params.totalShardNum"));
        totalColumnNum= Integer.valueOf(p.getProperty("params.totalColumnNum"));
        shardKeyIndex=Integer.valueOf(p.getProperty("params.shardKeyIndex"));

        encoding = p.getProperty("params.encoding");
        separator = p.getProperty("params.separator");
        enclosedChar = p.getProperty("params.enclosedChar");
        outputDir = p.getProperty("params.outputDir");
        failedFileName = p.getProperty("params.failedFileName");

        System.out.println("-------------------Config INFO------------------");
        System.out.println("数据文件路径: "+filePath);
        System.out.println("shard的数量: " +totalShardNum);
        System.out.println("表的列数: " +totalColumnNum);
        System.out.println("shardKey所在列的下标: " +shardKeyIndex);
        System.out.println("数据文件的编码: "+encoding);
        System.out.println("每行记录中数据之间的分隔符: "+separator);
        System.out.println("每行记录中每个数据的包围符号: "+enclosedChar);
        System.out.println("输出文件的目录： "+ outputDir);
        System.out.println("错误行的文件名： " + failedFileName);
        System.out.println("------------------------------------------------");

        shardEvaluater = new ShardEvaluater(totalShardNum);
    }

    /**
     * 去除分隔符
     * */
    public static String trimStartAndEnd(String value, String enclosedCh){
        if(value != null && value.length() >= 2 && enclosedCh!=null && !enclosedCh.equals("")){
            String start = value.charAt(0)+"";
            int length = value.length();
            String end = value.charAt(length-1)+"";
            if(start.equals(enclosedCh) && end.equals(enclosedCh))
                value=value.substring(1, length-1);
        }
        return value;
    }

    public static void main(String[] args) throws IOException {

        if(args.length != 0) {
            /**read the config file*/
            loadConfig(args[0]);
            // loadConfig("/home/transwarp/IdeaProjects/loadKunDB/src/main/java/params.properties");
            /**
             * open totalShardNum files
             * */
            BufferedOutputStream[] fosArray = new BufferedOutputStream[totalShardNum];
            for(int i = 0 ; i < fosArray.length; i++){
                String shardfilename = outputDir +"/shard"+i+".txt";
                fosArray[i] = new BufferedOutputStream(new FileOutputStream(shardfilename), (1<<20) * 16) ;
            }

            String errorfilename = outputDir + "/"+failedFileName;

            int lineCount = 0;

            /**
             * read input files
             * */
            try (
                 FileOutputStream errorFile = new FileOutputStream(errorfilename);
                 BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), encoding))
            ) {
                String lineTxt;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] record = lineTxt.split(separator);
                    if(record.length > totalColumnNum){
                        /**
                         * output invalid data into failedFile.
                         * */
                        errorFile.write(lineTxt.getBytes(encoding));
                        errorFile.write('\n');
                        continue;
                    }

                    /**
                     * write data into files according to shard key.
                     * */
                    String shardKeyValue ="";
                    if (shardKeyIndex < record.length)
                         shardKeyValue = record[shardKeyIndex];
                    shardKeyValue = trimStartAndEnd(shardKeyValue,enclosedChar);
                    int shardId = shardEvaluater.calculateShard(shardKeyValue.getBytes(encoding));
                    fosArray[shardId].write(lineTxt.getBytes(encoding));
                    fosArray[shardId].write('\n');

                    if (++lineCount % 10000 == 0) {
                        System.out.print('.');
                    }
                }
            }

            /**
             * close totalShardNum files
             * */
            for(int i = 0 ; i < fosArray.length; i++){
                fosArray[i].close();
            }

            System.out.println();
            System.out.println("DONE");

        }else{
            System.out.println("ERROR: params are needed.");
        }
    }
}
