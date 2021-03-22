package nowcoder;
// 汇编语言中有一种移位指令叫做循环左移（ROL），现在有个简单的任务，就是用字符串模拟这个指令的运算结果。
// 对于一个给定的字符序列S，请你把其循环左移K位后的序列输出。
// 例如，字符序列S=”abcXYZdef”,要求输出循环左移3位后的结果，即“XYZdefabc”。是不是很简单？OK，搞定它！
public class Nowcoder43_1{
    // 方法2:使用String对象的substring方法
    // 思路:首先排除一些特殊情况,然后直接拼接并返回str.substring方法生成的子串
    // PS:String.substring(int beginIndex,int endIndex)方法中截取的字符串不包括endIndex对应的字符
    // 时间复杂度:O(n),空间复杂度:O(n)
    public String LeftRotateString(String str,int n) {
        // 排除特殊情况
        if(str == null || str.length()== 0 || n<=0 || str.length()<=n) return str;
        // 直接返回拼接子串
        return str.substring(n,str.length()) + str.substring(0,n);
    }
    
    // 如果不允许使用substring,则自己写一个
    private String Substring(String str,int beginIndex,int endIndex){
        return new String(str.toCharArray(),beginIndex,endIndex-beginIndex);
    }
}