package nowcoder;
// 写一个函数，求两个整数之和，要求在函数体内不得使用+、-、*、/四则运算符号。
public class Nowcoder48{
    // 方法1:模拟加法进位过程
    // 思路:由于计算机中所有的数字都是使用补码进行保存,因此不论是正正/正负/负负
    // 的两个数相加都可以直接对其二进制进行加法运算求得两个数之和,因为补码将减法
    // 转换成的加法运算(即补码的定义),因此只需要模拟两个数的按位求和运算即可.
    // 使用carry来表示各个bit位上是否有来自低位的进位1,使用xorSum来表示两个数的
    // 异或之和.其中异或和可以看做是非进位求和运算,而carry则表示每个bit位上的进位
    // 情况,将这两种运算分离,来模拟实际的按位加法运算.
    // 初始时:carry = (num1 & num2)<<1;xorSum = num1^num2;
    // 然后进行循环遍历,当有进位时则进行循环,循环时,temp=xorSum,xorSum^=carry;carry=(carry&temp)<<1;
    // 时间复杂度:O(n),空间复杂度:O(1)
    public int Add(int num1,int num2) {
        //计算异或之和以及进位值
        int xorSum = num1^num2;
        int carry = (num1 & num2) << 1;
        int temp;
        while(carry != 0){
            temp = xorSum;
            xorSum ^= carry;// 对异或和和进位数进行非进位求和
            carry = (carry & temp)<<1; // 对后续非进位求和保存进位值
        }
        return xorSum;
    }
}