package io.transwarp.loadToKunDB;

public class ByteArrayUtil {
    public static boolean matchesAt(
            byte[] bytes1, int offset1,
            byte[] bytes2){
        if (offset1 < 0 || offset1 + bytes2.length > bytes1.length) {
            return false;
        }
        if (bytes2.length == 1 && bytes1[offset1] == bytes2[0]){
            return true;
        }
        for (int i=0; i < bytes2.length; i++){
            if(bytes1[offset1+i]!=bytes2[i]){
                return false;
            }
        }
        return true;
    }
}
