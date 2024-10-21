package java.nowcoder;
// 牛客最近来了一个新员工Fish，每天早晨总是会拿着一本英文杂志，写些句子在本子上。
// 同事Cat对Fish写的内容颇感兴趣，有一天他向Fish借来翻看，但却读不懂它的意思。
// 例如，“student. a am I”。后来才意识到，这家伙原来把句子单词的顺序翻转了，
// 正确的句子应该是“I am a student.”。Cat对一一的翻转这些单词顺序可不在行，你能帮助他么？
public class Nowcoder44{
    // 方法1:双指针+字符数组
    // 思路:对字符串中的字符从后向前进行递归,创建等长字符数组用于保存变换后的字符.初始时
    // 双指针left和right同时指向原始字符数组最后的字符,cursor指向新字符数组的下一个字符位
    // 置,若right指向的是空格字符,则将此字符添加到新字符数组中,双指针同时左移1位,若right
    // 指向的不是空格字符,则移动left,直到left遇到空格字符或者等于-1时,将(left,right]之间
    // 的字符依次添加到新字符数组中,然后right=left,继续遍历,直到遍历结束.
    // PS:不要想着使用String.split(String regex)方法,再使用StringBuilder以此连接的方式.
    // 如果单词中间有多个连续空格,那么这种思路就会出错,因为无法保存空格数量,而且这种方
    // 看似简单,实则不满足面试要求.
    // 时间复杂度:O(n),空间复杂度:O(n)
    public String ReverseSentence(String str) {
        // 排除特殊情况
        if(str == null || str.length() <= 1) return str;
        // 定义双指针,创建辅助数据结构
        int len = str.length();
        int left = len-1, right = len-1, cursor = 0;
        char[] dest = new char[len];
        char[] source = str.toCharArray();
        // 遍历字符数组
        while(cursor<len){
            // 若双指针指向的是空格,则一次性将多个空格添加进新字符数组
            while(right >= 0 && source[right]==' '){
                dest[cursor++]=' ';
                right--;
                left--;
            }
            // 若左指针指向的是非空格,则一次性将left尽量向左移动
            while(left >= 0 && source[left]!=' ') left--;
            // 目前保证了right指向的是非空格,left要么指向空格,要么指向-1
            // 因此此时可以将(left,right]之间的字符全都放入新字符数组中
            for(int i=left+1;i<=right;i++){
                dest[cursor++]=source[i];
            }
            // 然后right赋值为left继续遍历
            right = left;
        }
        // 返回新字符数组生成的字符串
        return new String(dest);
    }
}