package java.nowcoder;
// 请实现一个函数用来匹配包括'.'和'*'的正则表达式。模式中的字符'.'表示任意一个字符，
// 而'*'表示它前面的字符可以出现任意次（包含0次）。 在本题中，匹配是指字符串的所有字符匹配整个模式。
// 例如，字符串"aaa"与模式"a.a"和"ab*ac*a"匹配，但是与"aa.a"和"ab*a"均不匹配
public class Nowcoder52_1{
    // 方法2:自底向上递归
    // 思路:方法2和方法1的思路大致相同,不过方法1是自顶向下(即从右往左),而方法2是自底向上(即从左往右)
    private char[] str;
    private char[] pattern;
    public boolean match(char[] str, char[] pattern){
        // 排除特殊情况
        if(str == null || pattern == null) return false;
        this.str = str;
        this.pattern = pattern;
        return solve(0,0);
    }
    
    // 递归方法
    private boolean solve(int i, int j){
        // 如果字符串和模式的字符都匹配完成,则返回true
        if(i == str.length && j == pattern.length) return true;
        // 否则,如果字符串未匹配完,而模式的字符已经匹配完,则返回false
        if(j == pattern.length) return false;
        // 如果模式的下一个字符为'*',则继续判断.
        if(j+1 < pattern.length && pattern[j+1] == '*'){
            // 如果字符串的字符未匹配完成,且字符串当前字符和模式当前字符匹配,则
            // 分别对模式当前字符重复0次和重复n次两种情况进行递归
            if(i != str.length && (str[i] == pattern[j] || pattern[j] == '.')){
                return solve(i,j+2) || solve(i+1,j);
            }
            // 否则则直接向后递归
            else return solve(i,j+2);
        }
        else{// 如果模式的下一个字符不为'*'
            // 如果当前字符相互匹配,则继续递归
            if(i != str.length && (str[i] == pattern[j] || pattern[j] =='.')){
                return solve(i+1,j+1);
            }
            // 否则返回false
            else return false;
        }
    }
}