package nowcoder;
// 写一个函数，求两个整数之和，要求在函数体内不得使用+、-、*、/四则运算符号。
public class Nowcoder48_1{
    // 方法2:同方法1
    // 思路:使用异或运算的性质a^b=b^a,a^b^b=a来简化方法1中的计算公式,避免声明temp变量
    // 时间复杂度:O(n),空间复杂度:O(1)
    public int Add(int num1,int num2) {
        // 计算异或之和和进位值
        int xorSum = num1^num2;
        int carry = num1&num2;
        // 当有进位时,则进行循环
        while(carry != 0){
            xorSum^=carry;
            carry = ((xorSum^carry)&carry)<<1;
        }
        // 返回结果
        return xorSum;
    }
}