package io.transwarp.loadToKunDB;

import org.junit.Test;

import static org.junit.Assert.*;

public class ByteArrayUtilTest {
    private byte[] bytes1= {12,34,56,78,90};
    private byte[] bytes2= {34,56,78};
    private byte[] bytes3= {12,34,56};
    private byte[] bytes4= {56,78,90};

    @Test
    public void testNormalCase(){
        assertTrue(ByteArrayUtil.matchesAt(bytes1,0,bytes3));
        assertFalse(ByteArrayUtil.matchesAt(bytes1,0,bytes2));
        assertTrue(ByteArrayUtil.matchesAt(bytes1, 1, bytes2));
        assertFalse(ByteArrayUtil.matchesAt(bytes1, 1, bytes3));
        assertTrue(ByteArrayUtil.matchesAt(bytes1, 2, bytes4));
        assertFalse(ByteArrayUtil.matchesAt(bytes1, 2, bytes2));
    }

    @Test
    public void testOutOfBoundCase(){
        assertFalse(ByteArrayUtil.matchesAt(bytes1,-1,bytes2));
        assertFalse(ByteArrayUtil.matchesAt(bytes1,3,bytes2));
    }
}
