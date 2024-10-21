package java.nowcoder;

import java.util.Random;

public class Solution {
    public static void main(String[] args) {
        int a = new Random().nextInt(100);
        System.out.println(-a);
        System.out.println((~a) + 1);
    }
}