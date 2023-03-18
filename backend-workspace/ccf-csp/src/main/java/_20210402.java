package main.java;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class _20210402 {
    public static void main(String[] args) throws IOException {
        // input
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        String[] inputs = bf.readLine().split(" ");
        int n = Integer.parseInt(inputs[0]);
        int l = Integer.parseInt(inputs[1]);
        int r = Integer.parseInt(inputs[2]);
        int t = Integer.parseInt(inputs[3]);

        int[][] grayArray = new int[n][n];
        for (int i = 0; i < n; i++) {
            inputs = bf.readLine().split(" ");
            for (int j = 0; j < inputs.length; j++) {
                grayArray[i][j] = Integer.parseInt(inputs[j]);
            }
        }

        // traverse
        int count = 0;
        for (int i = 0; i < grayArray.length; i++) {
            for (int j = 0; j < grayArray[i].length; j++) {
                if (adjacentGrayAvg(grayArray, i, j, r) <= t) {
                    count += 1;
                }
            }
        }

        // output
        System.out.println(count);
    }

    private static double adjacentGrayAvg(int[][] values, int x, int y, int range) {
        // new start and end coordinates
        int x1 = x - range, x2 = x + range, y1 = y - range, y2 = y + range;
        int graySum = 0, count = 0;

        for (int i = x1; i <= x2; i++) {
            for (int j = y1; j <= y2; j++) {
                // if cross the bound of gray matrix
                if (i < 0 || j < 0 || i >= values.length || j >= values[i].length) {
                    continue;
                }
                count += 1;
                graySum += values[i][j];
            }
        }

        // return
        return graySum * 1.0 / count;
    }
}
