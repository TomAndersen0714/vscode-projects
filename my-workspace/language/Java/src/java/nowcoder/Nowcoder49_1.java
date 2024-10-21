package java.nowcoder;
// 将一个字符串转换成一个整数，要求不能使用字符串转换整数的库函数。 
// 数值为0或者字符串不是一个合法的数值则返回0
// 示例输入:
// +2147483647
// 1a33
// 输出:
// 2147483647
// 0
public class Nowcoder49_1{
    // 方法2:同方法1一样,遍历输入字符串对应的字符数组
    // 思路:方法1虽然已被AC(accept),但是并未考虑整数溢出这一特殊情况,
    // 因此在方法2中进行修正.
    // 时间复杂度:O(n),空间复杂度:O(1)
    public int StrToInt(String str) {
        // 排除特殊情况
        if(str == null || str.length() == 0) return 0;
        // 定义变量(符号位/已识别的数/开始遍历位置)
        int symbol = 1, num = 0, cursor = 0;
        char[] chars = str.toCharArray();
        // 获取可能存在的正负号
        if(chars[0] == '-'){
            symbol = -1;
            cursor ++;
        }else if(chars[0] == '+') cursor++;
        // 从开始位置遍历字符数组
        for(;cursor<chars.length;cursor++){
            // 判断是否会发生溢出
            if(num > Integer.MAX_VALUE/10 || 
                (num == Integer.MAX_VALUE/10 && (chars[cursor] - '0')>((symbol+1)/2+7)))
                return 0;
            // 如果当前字符为数字,则加入到已识别数字末尾
            if(chars[cursor] >= '0' && chars[cursor] <= '9'){
                num = num*10 + (chars[cursor] - '0');
            }else return 0;// 如果当前字符为非数字,则直接返回0
        }
        
        // 最后返回识别结果
        return symbol*num;
    }
}