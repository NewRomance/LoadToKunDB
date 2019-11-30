package io.transwarp.loadToKunDB;

public class Line {
    public byte[] bytes;
    public int size;

    public Line(int lineCap){
        bytes = new byte[lineCap];
    }
}
