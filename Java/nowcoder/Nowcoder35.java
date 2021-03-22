package nowcoder;
// 在数组中的两个数字，如果前面一个数字大于后面的数字，则这两个数字组成一个逆序对。
// 输入一个数组,求出这个数组中的逆序对的总数P。并将P对1000000007取模的结果输出。 即输出P%1000000007
public class Nowcoder35{
    // 方法1:双重便利(时间复杂度太高,未accept)
    // 思路:直接进行双重遍历,遍历数组时候,每次向前寻找大于当前数的个数,即当前数
    // 作为逆序数对的第二个元素的逆序数对个数,数组遍历完成后将逆序数对个数%1000000007返回即可
	// 时间复杂度:O(n^2)
    public int InversePairs(int [] array) {
        // 排除特殊情况
        if(array == null || array.length <= 1) return 0;
        // 设置统计变量,统计逆序对个数
        int count = 0;
        // 双重遍历,统计逆序数对个数
        for(int i=0; i<array.length; i++){
            for(int j=i-1; j>=0; j--){
                if(array[j] > array[i]) count++;
				count%=1000000007;
            }
        }
        // 返回
        return count%1000000007;
    }
}