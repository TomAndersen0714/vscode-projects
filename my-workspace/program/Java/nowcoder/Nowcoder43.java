package nowcoder;
// 汇编语言中有一种移位指令叫做循环左移（ROL），现在有个简单的任务，就是用字符串模拟这个指令的运算结果。
// 对于一个给定的字符序列S，请你把其循环左移K位后的序列输出。
// 例如，字符序列S=”abcXYZdef”,要求输出循环左移3位后的结果，即“XYZdefabc”。是不是很简单？OK，搞定它！
public class Nowcoder43{
    // 方法1:使用StringBuilder来辅助实现
    // 思路:主要使用了StringBuilder类的append(char[] str, int offset, int len)方法来实现,此方法
    // 支持添加字符数组中的指定Offset开始的指定长度len的字符到StringBuilder中.
    // 时间复杂度:O(n),空间复杂度:O(n),n为字符串长度
    public String LeftRotateString(String str,int n) {
        // 排除特殊情况
        if(str == null || str.length()==0 || n<=0 || str.length()<=n) return str;
        // 创建StringBuilder
        StringBuilder sb = new StringBuilder();
        sb.append(str.toCharArray(),n,str.length()-n);
        sb.append(str.toCharArray(),0,n);
        return sb.toString();
    }
}