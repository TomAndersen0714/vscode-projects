package test;

import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;

import main.java._20210401;

public class _20210401Test {
    public static void main(String[] args) {
        String inputStr = "7 11 8\n0 7 0 0 0 7 0 0 7 7 0\n7 0 7 0 7 0 7 0 7 0 7\n7 0 0 0 7 0 0 0 7 0 7\n7 0 0 0 0 7 0 0 7 7 0\n7 0 0 0 0 0 7 0 7 0 0\n7 0 7 0 7 0 7 0 7 0 0\n0 7 0 0 0 7 0 0 7 0 0";
        String outputStr = "";
        System.setIn(new BufferedInputStream(new ByteArrayInputStream(inputStr.getBytes())));

        try {
            _20210401.main(args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
