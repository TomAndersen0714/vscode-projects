package java.nowcoder;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
// 输入一个字符串,按字典序打印出该字符串中字符的所有排列。
// 例如输入字符串abc,则打印出由字符a,b,c所能排列出来的所有字符串abc,acb,bac,bca,cab和cba。
// 输入一个字符串,长度不超过9(可能有字符重复),字符只包括大小写字母。
public class Nowcoder27{
    // 方法1:递归回溯
    // 使用HashMap统计词频
    // 然后进行递归回溯,递归回溯的参数列表为(char[] chars,int level)
    // 每次递归时,遍历HashMap的keySet,尝试从中取出字符加入到char数组当前位置
    // 并将对应的剩余字符数-1,然后进入下层遍历,当遍历到最后一层时将当前char数
    // 组添加到返回列表中,每次递归结束时,将当前使用的字符数恢复.
    // 最后返回时,对列表进行排序
    private HashMap<Character,Integer> charToNums = new HashMap<>();// 词频统计
    private ArrayList<String> res = new ArrayList<>();// 返回列表
    public ArrayList<String> Permutation(String str) {
        // 排除特殊情况
        if(str == null || str.length() == 0) return res;
        // 统计词频
        for(char c:str.toCharArray()){
            charToNums.put(c,charToNums.getOrDefault(c,0)+1);
        }
        // 递归回溯,获取字符全排列
        backtracking(new char[str.length()],0);
        // 返回全排列列表
        Collections.sort(res);
        return res;
    }
    
    private void backtracking(char[] chars, int level){
        // 设置递归出口1
        if(level == chars.length){
            res.add(new String(chars));
            return;
        }
        // 进行本次递归
        for(char c:charToNums.keySet()){
            int num = charToNums.get(c);
            // 如果当前字符无剩余,则跳过此字符
            if(num == 0) continue;
            // 如果还有剩余,则将其加入到数组当前位置,对应数量-1,并进行下一层遍历
            chars[level] = c;
            charToNums.put(c,num-1);
            // 进行下一层递归
            backtracking(chars,level+1);
            // 当前字符的全排列结束后,将对应数量恢复
            charToNums.put(c,num);
        }
    }
}