package java.nowcoder;
import java.util.Collections;
import java.util.ArrayList;
// 输入一个字符串,按字典序打印出该字符串中字符的所有排列。
// 例如输入字符串abc,则打印出由字符a,b,c所能排列出来的所有字符串abc,acb,bac,bca,cab和cba。
// 输入一个字符串,长度不超过9(可能有字符重复),字符只包括大小写字母。
public class Nowcoder27_2{
    // 方法3:递归回溯
    // 首先获取字符数组,然后对其进行递归获取其全排列,然后将结果集排序后返回
    // 每次递归时,尝试将首字符与所有字符进行交换,如果某字符在之前已经交换过,
    // 则跳过,否则交换首字符与当前字符,然后进行下一层递归,下一层递归结束后,
    // 再将位置还原,然后继续尝试交换下一个字符,直到当前首字符之后的所有字符
    // 都遍历完成,每次递归时,若当前首字符已经是最后字符,则直接将当前数组结
    // 果存入返回列表,结束当前层递归.
    ArrayList<String> res = new ArrayList<>(); // 返回列表
    public ArrayList<String> Permutation(String str) {
        // 排除特殊情况
        if(str == null || str.length() == 0) return res;
        // 进行递归获取全排列
        getPermutation(str.toCharArray(),0,str.length()-1);
        // 给列表排序,然后返回列表
        Collections.sort(res);
        return res;
    }
    
    // 递归获取全排列
    private void getPermutation(char[] array,int begin,int end){
        // 设置递归出口
        if(begin == end){
            res.add(new String(array));
            return;
        }
        // 固定首字符,直接进行下一层递归
        getPermutation(array,begin+1,end);
        // 尝试交换首字符和后续字符,需要避免重复交换
        for(int i=begin+1; i<=end; i++){
            if(!containsElement(array,array[i],begin,i-1)){
                // 交换首字符和当前字符
                swap(array,begin,i);
                // 固定当前首字符,进行下一层递归s
                getPermutation(array,begin+1,end);
                // 下一层递归结束后,将顺序还原
                swap(array,begin,i);
            }
        }
    }
    
    // 判断指定数组指定范围内是否包含指定元素
    private boolean containsElement(char[] array,char e,int begin, int end){
        for(int i=begin; i<=end; i++){
            if(array[i] == e) return true;
        }
        return false;
    }
    
    // 交换数组中指定元素
    private void swap(char[] array,int a,int b){
        char temp = array[a];
        array[a] = array[b];
        array[b] = temp;
    }
}