package main.java;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class _20210401 {
    public static void main(String[] args) throws IOException {
        // input
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        String[] inputs = bf.readLine().split(" ");
        int n = Integer.parseInt(inputs[0]);
        int m = Integer.parseInt(inputs[1]);
        int l = Integer.parseInt(inputs[2]);

        // traverse
        int[] counts = new int[l];
        for (int i = 0; i < n; i++) {
            inputs = bf.readLine().split(" ");
            for (int j = 0; j < inputs.length; j++) {
                int idx = Integer.parseInt(inputs[j]);
                counts[idx] += 1;
            }
        }

        // output
        StringBuilder sb = new StringBuilder();
        for (int count : counts) {
            sb.append(count);
            sb.append(' ');
        }
        sb.deleteCharAt(sb.length() - 1);
        System.out.println(sb);
    }
}