package nowcoder;
// 在一个字符串(0<=字符串长度<=10000，全部由字母组成)中找到第一个只出现一次的字符,并返回它的位置, 如果没有则返回 -1（需要区分大小写）.
public class Nowcoder34_1{
    // 方法2:迭代(使用次数统计数组)
    // 和方法1不同,方法2的思路很简单,首先遍历字符串对应的字符数组,统计每个字符出现的次数
    // 然后进行二次遍历,获取其中出现次数只有一次的字符的首个位置
    public int FirstNotRepeatingChar(String str) {
        // 排除特殊情况
        if(str == null || str.length() == 0) return -1;
        // 创建定长字符次数统计数组
        int[] counts = new int['z'-'A'+1];
        // 首次遍历字符数组,统计各个字符的出现次数
        int len = str.length();
        for(int i=0; i<len; i++){
            counts[str.charAt(i)-'A']++;
        }
        // 二次遍历字符数组,如果当前字符出现次数为1,则返回此字符的位置
        for(int i=0; i<len; i++){
            if(counts[str.charAt(i)-'A'] == 1) return i;
        }
        // 若无出现次数为1的字符,则返回-1
        return -1;
    }
}