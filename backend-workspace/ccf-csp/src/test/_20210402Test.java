package test;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.BufferedInputStream;

import main.java._20210402;

public class _20210402Test {
    public static void main(String[] args) throws IOException {
        String inputStr = "4 16 1 6\n" +
                "0 1 2 3\n" +
                "4 5 6 7\n" +
                "8 9 10 11\n" +
                "12 13 14 15";
        System.setIn(new BufferedInputStream(new ByteArrayInputStream(inputStr.getBytes())));
        _20210402.main(args);
    }
}
