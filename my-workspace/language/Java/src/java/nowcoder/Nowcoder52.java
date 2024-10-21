package java.nowcoder;
// 请实现一个函数用来匹配包括'.'和'*'的正则表达式。模式中的字符'.'表示任意一个字符，
// 而'*'表示它前面的字符可以出现任意次（包含0次）。 在本题中，匹配是指字符串的所有字符匹配整个模式。
// 例如，字符串"aaa"与模式"a.a"和"ab*ac*a"匹配，但是与"aa.a"和"ab*a"均不匹配
public class Nowcoder52{
    // 方法1:自顶向下递归
    // 思路:添加一个递归方法solve,每次递归时,先判断字符串字符和模式字符是否都已经用尽,若同时用尽则
    // 返回true,若只是匹配字符用尽则返回false,若只是字符串字符用尽则还需要进行判断,例如模式字符串剩下
    // ".*",则还需要进行判断.
    // 然后判断,若字符串还未用尽,且字符串和当前模式当前字符匹配,则继续递归求解.
    // 若模式当前字符为'*',则当字符串还未用尽,且字符串和模式前一个字符匹配,则
    // 分别将模式前一个字符视为出现0次或者n次,进行求解.若字符串和模式前一个字
    // 符不匹配,则直接跳过模式两个字符,继续递归求解.
    // 若之前的条件都不满足,则说明,字符串当前字符和模式无法匹配,直接返回false;
    private char[] str;
    private char[] pattern;
    public boolean match(char[] str, char[] pattern){
        // 排除特殊情况
        if(str == null || pattern == null) return false;
        this.str = str;
        this.pattern = pattern;
        return solve(str.length - 1,pattern.length - 1);
    }
    
    // 递归方法
    private boolean solve(int i,int j){
        // 如果字符串和模式字符同时用尽,则匹配成功返回true
        if(i == -1 && j == -1) return true;
        // 如果字符串未匹配完成,模式字符已经用尽,则返回false
        else if(j == -1) return false;
        // 如果字符串未匹配完成,且字符串当前字符和模式当前字符相同,则向下递归
        if(i != -1 && (str[i] == pattern[j] || pattern[j] == '.')){
            return solve(i-1,j-1);
        }
        // 否则,如果模式当前字符为'*'
        else if(pattern[j] == '*'){
            // 如果字符串字符未匹配完成,且和匹配模式前一个字符匹配,则
            // 分别递归求解当前一个字符出现0次和出现n次的情况.
            if(i != -1 && (str[i] == pattern[j-1] || pattern[j-1] == '.')){
                return solve(i,j-2) || solve(i-1,j);
            }
            else return solve(i,j-2);
        }
        // 若前面都不满足,则表明字符串和模式无法匹配
        return false;
    }
}