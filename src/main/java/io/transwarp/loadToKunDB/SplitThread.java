package io.transwarp.loadToKunDB;

import org.apache.commons.pool2.ObjectPool;

import java.io.*;

import java.util.concurrent.BlockingQueue;

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

    public SplitThread(
            BlockingQueue<LineList> queue,
            ObjectPool<LineList> pool,
            int id,
            SplitTextConfig.InputConfig inputConfig,
            SplitTextConfig.OutputConfig outputConfig) throws UnsupportedEncodingException {
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

        setName("split-thread-" + id);
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
            for(int i = 0 ; i < bosArray.length; i++){
                String shardfilename = this.shardDirs[i] +"/"+getName()+".txt";
                bosArray[i] = new BufferedOutputStream(new FileOutputStream(shardfilename), (1<<20) * 16) ;
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
                        // TODO process line
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
                            // TODO put into error file
                            errorFile.write(bytes,0,size);
                        }

                        if (shardKeyColumnIndex == columnNum - 1) {
                            shardKeyColumnFrom = curColFrom;
                            shardKeyColumnSize = size - lineEnd.length - curColFrom;
                        }

                        // TODO calc md5 and write shard file
                        // trim enclosedChar if it exists.
                        if (enclosedChar.length != 0
                                && ByteArrayUtil.matchesAt(bytes,shardKeyColumnFrom,enclosedChar)
                                && ByteArrayUtil.matchesAt(bytes,shardKeyColumnFrom + shardKeyColumnSize - enclosedChar.length,enclosedChar)){
                            shardKeyColumnSize -= 2 * enclosedChar.length;
                            shardKeyColumnFrom += enclosedChar.length;
                        }
                        byte[] shardKeyColumn = new byte[shardKeyColumnSize];
                        System.arraycopy(bytes,shardKeyColumnFrom,shardKeyColumn,0,shardKeyColumnSize);

                        bosArray[shardEvaluater.calculateShard(shardKeyColumn)].write(bytes,0,size);

                        if(++lineNum % 10000 == 0) {
                            System.out.print(".");
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

            long took = System.currentTimeMillis() - start;
            System.out.println("split thread " + id + " processed " + lineNum + " lines in " +
                    took + " ms = " + (lineNum * 1000 / took) + " lines/s (" + errorLineNum + " error lines)" +
                    " , processTime "+ processTime +"ms");

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
