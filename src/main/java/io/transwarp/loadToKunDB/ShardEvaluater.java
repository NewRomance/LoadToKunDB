package io.transwarp.loadToKunDB;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Map;

public class ShardEvaluater {
    private static Map<Integer, Integer> SHIFT_MAP = ImmutableMap.<Integer, Integer>builder()
            .put(1, 8)
            .put(2, 7)
            .put(4, 6)
            .put(8, 5)
            .put(16, 4)
            .put(32, 3)
            .put(64, 2)
            .put(128, 1)
            .put(256, 0)
            .build();

    private final int shiftNum;

    public ShardEvaluater(int totalShard) {
        Integer shiftNum = SHIFT_MAP.get(totalShard);
        if (shiftNum == null) {
            throw new IllegalArgumentException("invalid totalShard: " + totalShard);
        }

        this.shiftNum = shiftNum;
    }

    public int calculateShard(byte[] keyspaceId) {
        byte[] hash = calcHash(keyspaceId);
        return (hash[0] & 0xFF) >>> shiftNum;
    }

    private byte[] calcHash(byte[] b) {
        // using md5 currently
        return DigestUtils.md5(b);
    }
}
