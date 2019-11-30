package io.transwarp.loadToKunDB;

public class SplitTextConfig {
  public InputConfig input;
  public RunConfig run;
  public OutputConfig output;

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
    public String errorDir;
  }
}
