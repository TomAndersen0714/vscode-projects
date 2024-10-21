package java.nowcoder;
import java.util.Arrays;
// 在一个字符串(0<=字符串长度<=10000，全部由字母组成)中找到第一个只出现一次的字符,并返回它的位置, 如果没有则返回 -1（需要区分大小写）.
public class Nowcoder34{
    // 方法1:迭代(使用访问标记数组+首次出现位置统计数组)
    // 因为规定了只会出现英文字符,因此可以创建定长的二次访问标记数组,以及首次出现位置统计数组
    // 其中首次出现位置统计数组中的初始值设置为-1,于是当位置数组中的值为-1时则表明对应字符之前
    // 未出现,否则表明出现了,而二次访问标记数组为true时,则表明出现了2次以上,两者配合来统计只出现
    // 1次的字符中最靠前的位置
    public int FirstNotRepeatingChar(String str) {
        // 排除特殊情况
        if(str == null || str.length() == 0) return -1;
        // 创建首次位置数组,二次访问标记数组
        int[] firstLoc = new int['z'-'A'+1];
        boolean[] isTwice = new boolean['z'-'A'+1];
        // 给位置数组设置初始值
        Arrays.fill(firstLoc,-1);
        // 遍历字符串的字符数组
        int len = str.length(),loc;
        for(int i=0; i<len; i++){
            loc = str.charAt(i)-'A';
            // 如果是首次出现,则记录其位置,否则设置为2次出现
            if(firstLoc[loc] == -1) firstLoc[loc] = i;
            else isTwice[loc] = true;
        }
        // 遍历二次访问标记数组,获取只出现一次的位置的最小值
        int index = -1;
        for(int i=0; i<isTwice.length; i++){
            if(!isTwice[i] && firstLoc[i]!=-1){
                if(index == -1) index = firstLoc[i];
                else index = Math.min(index,firstLoc[i]);
            }
        }
        // 返回出现次数只有一次的首个字符位置
        return index;
    }
}