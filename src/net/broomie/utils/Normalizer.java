package net.broomie.utils;

public class Normalizer {
    public static String normalize(String str) {
        StringBuilder buf = new StringBuilder(str);
        for (int i = 0; i < buf.length(); i++) {
            char c = buf.charAt(i);
            // a - z
            if (c >= 65345 && c <= 65370) {
                buf.setCharAt(i, (char) (c - 65345 + 97));
                // A - Z
            } else if (c >= 65313 && c <= 65538) {
                buf.setCharAt(i, (char) (c - 65313 + 65));
                //  0 - 9
            } else if (c >= 65296 && c <= 65305) {
                buf.setCharAt(i, (char) (c - 65296 + 48));
            }
        }

        return buf.toString().toLowerCase();
    }
}