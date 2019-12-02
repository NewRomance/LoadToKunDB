package io.transwarp.loadToKunDB;

import java.util.List;

public class SplitTextConfig {
  public InputConfig input;
  public RunConfig run;
  public OutputConfig output;
  public InsertConfig insert;

  public static class InputConfig {
    public String filePath;
    public String encoding;
    public int columnNum;
    public int shardKeyColumnIndex;
    public FormatConfig format;

    public static class FormatConfig {
      public String linesTerminatedBy;
      public String fieldsTerminatedBy;
      public String enclosedChar;
    }
  }

  public static class RunConfig {
    public int readBufSize;
    public int lineBufSize;
    public int lineListSize;
    public double splitParallelFactor;
    public int splitQueueSize;
  }

  public static class OutputConfig {
    public int shardNum;
    public String[] shardDirs;
    public long segmentSize;
    public int writeBufSize;
    public String errorDir;
  }

  public static class InsertConfig {
    public List<Shard> shards;
    public String mysqlCmd;
    public String sql;

    public static class Shard {
      public String masterHost;
      public int mysqlPort;
    }
  }
}
