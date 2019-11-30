package io.transwarp.loadToKunDB;


public class LineList {
    Line[] lines;
    int size;

    public LineList(int listCap, int lineCap){
        lines = new Line[listCap];
        for(int i = 0; i < listCap; i++){
            lines[i]= new Line(lineCap);
        }
    }
}
