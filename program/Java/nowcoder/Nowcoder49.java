package nowcoder;
// 将一个字符串转换成一个整数，要求不能使用字符串转换整数的库函数。 
// 数值为0或者字符串不是一个合法的数值则返回0
// 示例输入:
// +2147483647
// 1a33
// 输出:
// 2147483647
// 0
public class Nowcoder49{
    // 方法1:遍历字符串
    // 思路:直接获取输入字符串数组,使用symbol保存正负,num保存已经识别的数字,cursor表示
    // 下一个识别的字符索引下标.首先处理首位字符,如果为正负号,则将symbol设置成1或者-1,并
    // 且cursor++,否则symbol=1,cursor不变.然后从cursor开始遍历字符数组,若当前字符为数字
    // 字符,则将其计算进已经识别的数字中,若当前字符不是数字字符,则直接返回0,表示无法识别
    // 此字符.遍历结束后,返回已经识别的数字与正负号标识的乘积,即为此字符串代表的数值.
    public int StrToInt(String str) {
        // 排除特殊情况
        if(str == null || str.length() == 0) return 0;
        // 定义变量(符号位/已识别的数/开始遍历位)
        int symbol = 1, num = 0,cursor = 0;
        char[] chars = str.toCharArray();
        // 获取可能存在的符号位,如果存在则将开始游标右移
        if(chars[0] == '-'){
            symbol = -1;
            cursor ++;
        }else if(chars[0] == '+') cursor++;
        // 从开始位置遍历字符数组
        for(; cursor<chars.length; cursor++){
            // 如果当前字符为数字
            if(chars[cursor] >= '0' && chars[cursor]<='9'){
                num = num*10 + (chars[cursor] - '0');
            }else return 0;// 如果当前字符非数字,直接返回0,无法识别
        }
        // 返回识别结果
        return symbol*num;
    }
}