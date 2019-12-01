package io.transwarp.loadToKunDB;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SplitTextParallelMain {
    public static void main(String[] args) throws Exception {
        SplitTextConfig config = loadConfig(args[0]);
        SplitTextConfig.InputConfig inputConfig = config.input;
        SplitTextConfig.RunConfig runConfig = config.run;
        SplitTextConfig.OutputConfig outputConfig = config.output;

        ensureDirsCreated(outputConfig.shardDirs, outputConfig.errorDir);

        ObjectPool<LineList> pool = new GenericObjectPool<>(new LineListFactory(runConfig.lineListSize, runConfig.lineBufSize));

        // create blocking queues and threads for splitting
        int splitThreadNum = Math.max((int)Math.ceil(
                Runtime.getRuntime().availableProcessors() * runConfig.splitParallelFactor
        ), 1);

        List<BlockingQueue<LineList>> splitQueues = new ArrayList<>();
        // read and process src text file
        try (InputStream inputStream = new FileInputStream(inputConfig.filePath)) {
            for (int i=0; i<splitThreadNum; ++i) {
                ArrayBlockingQueue<LineList> q = new ArrayBlockingQueue<>(runConfig.splitQueueSize);
                splitQueues.add(q);
                // create threads and run
                SplitThread thread = new SplitThread(q, pool, i, inputConfig, outputConfig, config.insert);
                thread.start();
            }

            readAndProcess(inputStream, inputConfig, runConfig, splitThreadNum, splitQueues, pool);

        } finally {
            // give split threads poison pill
            for (BlockingQueue<LineList> queue: splitQueues){
                LineList poisonPill = pool.borrowObject();
                poisonPill.size = -1;
                queue.put(poisonPill);
            }
        }
    }

    private static void readAndProcess(
            InputStream inputStream,
            SplitTextConfig.InputConfig inputConfig,
            SplitTextConfig.RunConfig runConfig,
            int splitThreadNum,
            List<BlockingQueue<LineList>> splitQueues,
            ObjectPool<LineList> pool) throws Exception {
        String encoding = inputConfig.encoding;
        byte[] lineEnd = inputConfig.format.linesTerminatedBy.getBytes(encoding);

        long start = System.currentTimeMillis();
        long lineNum = 0;

        int n;
        byte[] buf = new byte[runConfig.readBufSize];

        LineList lineList = pool.borrowObject();
        int lineListCur = 0;
        byte[] lineBuf = lineList.lines[0].bytes;
        int lineCur = 0;
        while ((n = inputStream.read(buf)) != -1) {
            for (int i=0; i<n; ++i) {
                if (lineCur >= runConfig.lineBufSize) {
                    System.out.println("The current line is too long to be put into the lineBuffer, please set the lineBufSize more bigger.");
                    System.exit(0);
                }
                lineBuf[lineCur] = buf[i];

                boolean isEnd = lineBuf[lineCur] == lineEnd[lineEnd.length-1] &&
                        ByteArrayUtil.matchesAt(
                                lineBuf,lineCur - lineEnd.length + 1,
                                lineEnd);

                if (isEnd) {
                    lineNum++;

                    Line line = lineList.lines[lineListCur];
                    line.size = lineCur+1;
                    lineCur = 0;

                    lineListCur++;

                    if (lineListCur == runConfig.lineListSize) {
                        lineList.size = lineListCur;

                        // send to BlockingQueue using round robin
                        int qid = (int) ((lineNum / runConfig.lineListSize) & Integer.MAX_VALUE) % splitThreadNum;
                        splitQueues.get(qid).put(lineList);
                        lineList = pool.borrowObject();

                        lineListCur = 0;
                    }

                    lineBuf = lineList.lines[lineListCur].bytes;
                } else {
                    lineCur++;
                }
            }
        }

        long took = System.currentTimeMillis() - start;
        System.out.println("\nmain thread " + lineNum + " lines / " + took + " ms = " + 1000*lineNum/took + " lines/s");

        // TODO expand linebuf if line
    }

    private static void ensureDirsCreated(String[] shardDirs, String errorDir) throws IOException {
        for (String fileDir : shardDirs) {
            Files.createDirectories(Paths.get(fileDir));
        }
        Files.createDirectories(Paths.get(errorDir));
    }

    private static SplitTextConfig loadConfig(String confFilePath) throws IOException {
        SplitTextConfig config;
        try (InputStream inputStream = new FileInputStream(confFilePath)) {
            config = new Yaml(new Constructor(SplitTextConfig.class))
                    .load(inputStream);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        System.out.println("config: ");
        System.out.println(objectMapper.writeValueAsString(config));

        return config;
    }
}
