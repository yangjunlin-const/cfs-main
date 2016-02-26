package com.buaa.cfs.other;

import java.util.regex.Pattern;

/**
 * Created by root on 2/25/16.
 */
public class TestNumChar {

    public static void main(String[] args) {
        String list = "/home/yjl/test/123211";
        list.hashCode();
        Pattern numPattern = Pattern.compile("[0-9]");
        Pattern charPattern = Pattern.compile("[a-zA-Z]");
        long result = 0L;
        char[] listChar = list.toCharArray();
        for (char c : listChar) {
            if (charPattern.matcher(String.valueOf(c)).matches()) {
                System.out.println(c);
                result += (int) c;
            } else if (numPattern.matcher(String.valueOf(c)).matches()) {
                System.out.println(c);
                result += (int) c;
            }
        }
        System.out.println(result);
        String preFile = TestNumChar.class.getResource("").getPath();
        System.out.println(preFile);

    }
}
