package io.transwarp.loadToKunDB;

import org.apache.commons.pool2.ObjectPool;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

public class SplitThread extends Thread {
    private final BlockingQueue<LineList> queue;
    private final ObjectPool<LineList> pool;
    private final int id;
    private final byte[] lineEnd;
    private final SplitTextConfig.InputConfig inputConfig;
    private final SplitTextConfig.OutputConfig outputConfig;

    public SplitThread(
            BlockingQueue<LineList> queue,
            ObjectPool<LineList> pool,
            int id,
            SplitTextConfig.InputConfig inputConfig,
            SplitTextConfig.OutputConfig outputConfig) throws UnsupportedEncodingException {
        this.queue = queue;
        this.pool = pool;
        this.id = id;
        this.inputConfig = inputConfig;
        this.outputConfig = outputConfig;
        lineEnd = inputConfig.format.linesTerminatedBy.getBytes(inputConfig.encoding);
        setName("split-thread-" + id);
    }

    @Override
    public void run() {
        try {
            System.out.println("split thread " + id + " started");

            long start = System.currentTimeMillis();
            long lineNum = 0, errorLineNum = 0;

            long processTime = 0;

            ShardEvaluater shardEvaluater = new ShardEvaluater(outputConfig.shardNum);
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
                        byte[] fieldTerminator = inputConfig.format.fieldsTerminatedBy.getBytes(inputConfig.encoding);
                        int shardKeyColumnIndex = inputConfig.shardKeyColumnIndex;
                        int columnNum = inputConfig.columnNum;

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

                        }

                        if (shardKeyColumnIndex == columnNum - 1) {
                            shardKeyColumnFrom = curColFrom;
                            shardKeyColumnSize = size - lineEnd.length - curColFrom;
                        }

                        // TODO calc md5 and write shard file
                        byte[] shardKeyColumn = new byte[shardKeyColumnSize];
                        System.arraycopy(bytes,shardKeyColumnFrom,shardKeyColumn,0,shardKeyColumnSize);

                        int shardIndex=shardEvaluater.calculateShard(shardKeyColumn);


                        ++lineNum;
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

            long took = System.currentTimeMillis() - start;
            System.out.println("split thread " + id + " processed " + lineNum + " lines in " +
                    took + " ms = " + (lineNum * 1000 / took) + " lines/s (" + errorLineNum + " error lines)" +
                    " , processTime "+ processTime +"ms");
        } catch (InterruptedException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
