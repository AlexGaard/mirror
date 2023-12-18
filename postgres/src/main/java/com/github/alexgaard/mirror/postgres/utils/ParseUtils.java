package com.github.alexgaard.mirror.postgres.utils;

import java.util.Arrays;

public class ParseUtils {

    public static short getShort(int fromIdx, byte[] bytes) {
        return (short) (bytes[fromIdx]<<8 | bytes[fromIdx + 1] & 0xFF);
    }

    public static int getInt(int fromIdx, byte[] bytes) {
        return ((bytes[fromIdx] & 0xFF) << 24) |
                ((bytes[fromIdx + 1] & 0xFF) << 16) |
                ((bytes[fromIdx + 2] & 0xFF) << 8 ) |
                ((bytes[fromIdx + 3] & 0xFF) << 0 );
    }

    public static long getLong(int fromIdx, byte[] bytes) {
        return ((long) (bytes[fromIdx]) << 56)
                + (((long) bytes[fromIdx + 1] & 0xFF) << 48)
                + ((long) (bytes[fromIdx + 2] & 0xFF) << 40)
                + ((long) (bytes[fromIdx + 3] & 0xFF) << 32)
                + ((long) (bytes[fromIdx + 4] & 0xFF) << 24)
                + ((bytes[fromIdx + 5] & 0xFF) << 16)
                + ((bytes[fromIdx + 6] & 0xFF) << 8)
                + (bytes[fromIdx + 7] & 0xFF);
    }

    public static char getChar(int fromIdx, byte[] bytes) {
      return (char) bytes[fromIdx];
    }

    public static byte[] getBytes(int fromIdx, int length, byte[] bytes) {
        return Arrays.copyOfRange(bytes, fromIdx, fromIdx + length);
    }

    public static byte[] toByteArray(String hexStr) {
        int len = hexStr.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexStr.charAt(i), 16) << 4)
                    + Character.digit(hexStr.charAt(i+1), 16));
        }
        return data;
    }

}
