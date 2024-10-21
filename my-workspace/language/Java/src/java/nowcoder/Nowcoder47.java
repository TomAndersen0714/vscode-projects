package java.nowcoder;
// 求1+2+3+...+n，要求不能使用乘除法、for、while、if、else、switch、case等关键字及条件判断语句（A?B:C）。
public class Nowcoder47{
    // 方法1:利用布尔运算中与运算的性质
    // 思路:利用布尔运算中与运算的性质,当左侧表达式为false时,不计算右侧表达式,当左侧
    // 表达式结果为true时,再计算右侧表达式,这样可以实现if语句的同等效果
    // 时间复杂度:O(n),空间复杂度:O(n)
    public int Sum_Solution(int n) {
        int sum = n;
        boolean temp = (n>1) && ((sum+=Sum_Solution(n-1))>0);
        return sum;
    }
}