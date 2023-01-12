package nowcoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
// 输入一个字符串,按字典序打印出该字符串中字符的所有排列。
// 例如输入字符串abc,则打印出由字符a,b,c所能排列出来的所有字符串abc,acb,bac,bca,cab和cba。
// 输入一个字符串,长度不超过9(可能有字符重复),字符只包括大小写字母。
public class Nowcoder27_1{
    // 方法2:递归
    // 首先获取所有的字符并将其排序,保证相同的字符相邻
    // 然后进行递归,递归结束后,将返回列表排序,最终返回全排列结果
    // 每次递归时,先直接固定当前首字符进行下一层递归,然后尝试将首字符
    // 与后续字符进行交换,必须保证交换字符不与首字符相同,且之前未与首
    // 字符交换过(由于除首字符之外的字符,相同的字符都相邻,因此只需要判
    // 断前一个字符是否与当前字符相同即可),则将首字符与之交换,并进入下
    // 一层递归,每次递归结尾,都需要将交换的字符还原.
    ArrayList<String> res = new ArrayList<>(); // 返回列表
    public ArrayList<String> Permutation(String str) {
        // 排除特殊情况
        if(str == null || str.length() == 0) return res;
        // 获取字符数组,并将其排序,保证除首字符外相同的字符相邻
        char[] array = str.toCharArray();
        Arrays.sort(array);
        // 对字符数组进行递归交换,获取其全排列
        getPermutation(array,0,array.length - 1);
        // 因为题目需要全排列升序排列,所以将返回列表排序后,再返回
        Collections.sort(res);
        return res;
    }
    
    // 递归获取全排列
    private void getPermutation(char[] array,int begin, int end){
        // 设置递归出口1
        if(begin == end){
            res.add(new String(array));
            return;
        }
        // 固定首字符,向下递归
        getPermutation(array,begin+1,end);
        // 遍历后序字符,交换首字符,向下递归
        for(int i=begin+1; i<=end; i++){
            // 如果当前字符和首字符相同,或者当前字符之前交换过,则直接跳过
            if(array[i]==array[begin] || array[i]==array[i-1]) continue;
            // 交换首字符与当前字符
            swap(array,begin,i);
            // 固定当前首字符,向下递归
            getPermutation(array,begin+1,end);
            // 当前交换递归结束,将位置还原
            swap(array,begin,i);
        }
    }
    
    private void swap(char[] array,int a,int b){
        char temp = array[a];
        array[a] = array[b];
        array[b] = temp;
    }
}