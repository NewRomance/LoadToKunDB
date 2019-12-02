package io.transwarp.loadToKunDB;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.pool2.ObjectPool;

import java.io.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class SplitThread extends Thread {
    private final BlockingQueue<LineList> queue;
    private final ObjectPool<LineList> pool;
    private final int id;

    private final int shardKeyColumnIndex;
    private final int columnNum;

    private final byte[] lineEnd;
    private final byte[] fieldTerminator;
    private final byte[] enclosedChar;

    private final int shardNum;
    private final String errorDir;
    private final String[] shardDirs;
    private final long segmentSize;
    private final int writeBufSize;

    private final ExecutorService insertPool;

    private final SplitTextConfig.InsertConfig insertConfig;

    public SplitThread(
        BlockingQueue<LineList> queue,
        ObjectPool<LineList> pool,
        int id,
        SplitTextConfig.InputConfig inputConfig,
        SplitTextConfig.OutputConfig outputConfig,
        SplitTextConfig.InsertConfig insertConfig) throws UnsupportedEncodingException {
        this.queue = queue;
        this.pool = pool;
        this.id = id;

        this.shardKeyColumnIndex = inputConfig.shardKeyColumnIndex;
        this.columnNum = inputConfig.columnNum;

        this.lineEnd = inputConfig.format.linesTerminatedBy.getBytes(inputConfig.encoding);
        this.fieldTerminator = inputConfig.format.fieldsTerminatedBy.getBytes(inputConfig.encoding);
        this.enclosedChar = inputConfig.format.enclosedChar.getBytes(inputConfig.encoding);

        this.shardNum = outputConfig.shardNum;
        this.errorDir = outputConfig.errorDir;
        this.shardDirs = outputConfig.shardDirs;
        this.segmentSize = outputConfig.segmentSize;
        this.writeBufSize = outputConfig.writeBufSize;

        this.insertConfig = insertConfig;

        setName("split-thread-" + id);

        this.insertPool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setDaemon(false)
            .setNameFormat(getName() + "-insert-pool-%d")
            .build());
    }

    @Override
    public void run() {
        try(FileOutputStream errorFile = new FileOutputStream(this.errorDir+"/"+getName()+".txt")) {
            System.out.println("split thread " + id + " started");

            long start = System.currentTimeMillis();
            long lineNum = 0, errorLineNum = 0;

            long processTime = 0;

            ShardEvaluater shardEvaluater = new ShardEvaluater(shardNum);

            /**
             * open totalShardNum files
             * */
            BufferedOutputStream[] bosArray = new BufferedOutputStream[this.shardNum];
            long[] segSizeArray = new long[this.shardNum];
            int[] segIndexArray = new int[this.shardNum];
            for(int i = 0 ; i < bosArray.length; i++){
                String shardfilename = getShardFilePath(i, 0);
                bosArray[i] = new BufferedOutputStream(new FileOutputStream(shardfilename), this.writeBufSize) ;
            }

            while (true) {
                LineList lineList = null;
                try {
                    lineList = queue.take();

                    long processStart = System.currentTimeMillis();
                    //judge poison pill
                    if (lineList.size == -1) {
                        break;
                    }

                    for(int i=0; i<lineList.size; i++){
                        // ---------------
                        // process line
                        // ---------------

                        Line line = lineList.lines[i];
                        byte[] bytes = line.bytes;
                        int size = line.size;

                        int delimCount = 0;
                        int shardKeyColumnFrom = 0, shardKeyColumnSize = 0;
                        int curColFrom = 0;
                        for(int j = 0; j < size - lineEnd.length; j++){
                            if(bytes[j] == fieldTerminator[0]
                                    && ByteArrayUtil.matchesAt(bytes,j,fieldTerminator)){
                                if(delimCount == shardKeyColumnIndex){
                                    shardKeyColumnFrom = curColFrom;
                                    shardKeyColumnSize = j - curColFrom;
                                }
                                delimCount ++;
                                curColFrom = j + fieldTerminator.length;
                                j += fieldTerminator.length-1;
                            }
                        }

                        if (delimCount+1 != columnNum) {
                            ++errorLineNum;
                            // put into error file
                            errorFile.write(bytes,0,size);
                            continue;
                        }

                        if (shardKeyColumnIndex == columnNum - 1) {
                            shardKeyColumnFrom = curColFrom;
                            shardKeyColumnSize = size - lineEnd.length - curColFrom;
                        }

                        // trim enclosedChar if it exists.
                        if (enclosedChar.length != 0
                                && ByteArrayUtil.matchesAt(bytes,shardKeyColumnFrom,enclosedChar)
                                && ByteArrayUtil.matchesAt(bytes,shardKeyColumnFrom + shardKeyColumnSize - enclosedChar.length,enclosedChar)){
                            shardKeyColumnSize -= 2 * enclosedChar.length;
                            shardKeyColumnFrom += enclosedChar.length;
                        }
                        byte[] shardKeyColumn = new byte[shardKeyColumnSize];
                        System.arraycopy(bytes,shardKeyColumnFrom,shardKeyColumn,0,shardKeyColumnSize);

                        int shardId = shardEvaluater.calculateShard(shardKeyColumn);
                        bosArray[shardId].write(bytes,0,size);
                        segSizeArray[shardId] += size;

                        if (segSizeArray[shardId] >= segmentSize) {
                            // rotation
                            System.out.println("rotating thread " + id + " shard " + shardId);

                            bosArray[shardId].close();
                            insertPool.submit(new InsertRunnable(
                                    insertConfig,
                                    getShardFilePath(shardId, segIndexArray[shardId]),
                                    shardId));

                            segIndexArray[shardId] += 1;
                            segSizeArray[shardId] = 0;
                            String shardFileName = getShardFilePath(shardId, segIndexArray[shardId]);
                            bosArray[shardId] = new BufferedOutputStream(new FileOutputStream(shardFileName), this.writeBufSize) ;
                        }
                    }

                    processTime += System.currentTimeMillis() - processStart;

                } finally {
                    // return object to pool
                    if (lineList != null) {
                        try {
                            pool.returnObject(lineList);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            /**
             * close totalShardNum files
             * */
            for(int i = 0 ; i < bosArray.length; i++){
                bosArray[i].close();
            }

            for (int i = 0; i < shardNum; ++i) {
                if (segSizeArray[i] > 0) {
                    insertPool.submit(new InsertRunnable(
                        insertConfig,
                        getShardFilePath(i, segIndexArray[i]),
                        i));
                }
            }
            insertPool.shutdown();

            long took = System.currentTimeMillis() - start;
            System.out.println("split thread " + id + " processed " + lineNum + " lines in " +
                    took + " ms = " + (lineNum * 1000 / took) + " lines/s (" + errorLineNum + " error lines)" +
                    " , processTime "+ processTime +"ms");

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }

    private String getShardFilePath(int shardId, int segIndex) {
        return shardDirs[shardId] + "/" + getName() + "-segment-" + segIndex + ".txt";
    }

    public static class InsertRunnable implements Runnable {
        private final SplitTextConfig.InsertConfig insertConfig;
        private final String fromFile;
        private final int shardId;

        public InsertRunnable(
                SplitTextConfig.InsertConfig insertConfig,
                String fromFile,
                int shardId) {
            this.insertConfig = insertConfig;
            this.fromFile = fromFile;
            this.shardId = shardId;
        }

        @Override
        public void run() {
            System.out.println("[" + fromFile + "] will transfer and insert");

            SplitTextConfig.InsertConfig.Shard shard = insertConfig.shards.get(shardId);
            String masterHost = shard.masterHost;

            // execute load data sql
            String sql = insertConfig.sql.replace("${infile}", fromFile);
            String mysqlCmd = insertConfig.mysqlCmd
                .replace("${host}", masterHost)
                .replace("${port}", Integer.toString(shard.mysqlPort))
                .replace("${sql}", sql);
            execCommand(mysqlCmd);
        }

        private void execCommand(String command) {
            try {
                System.out.println("[" + fromFile + "] execCommand: " + maskPassword(command));

                long start = System.currentTimeMillis();

                String[] cmdArray = new String[]{"bash", "-c", command};
                Process proc = Runtime.getRuntime().exec(cmdArray);

                proc.getOutputStream().close();
                StreamGobbler errorGobbler =
                    new StreamGobbler(proc.getErrorStream(), "[" + fromFile + "] ERROR");
                StreamGobbler outputGobbler = new
                    StreamGobbler(proc.getInputStream(), "[" + fromFile + "] OUTPUT");
                errorGobbler.start();
                outputGobbler.start();

                int exitVal = proc.waitFor();
                long took = System.currentTimeMillis() - start;
                System.out.println("[" + fromFile + "] ExitValue: " + exitVal + " (" + took + " ms)");

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        private static final Pattern PasswordPattern = Pattern.compile("-p[^o]\\S+");

        private String maskPassword(String command) {
            return PasswordPattern.matcher(command).replaceAll("-p***");
        }
    }
}
