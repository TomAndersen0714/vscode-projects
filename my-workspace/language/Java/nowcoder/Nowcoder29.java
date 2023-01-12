package nowcoder;
import java.util.ArrayList;
// 输入n个整数，找出其中最小的K个数。例如输入4,5,1,6,2,7,3,8这8个数字，则最小的4个数字是1,2,3,4。
// 注意:当没有k个数时,返回空列表
public class Nowcoder29{
    // 方法1:使用快排
    // 在使用快排思想对数组进行大致排序时,每次能够确定一个数在数组中的升序位置,并保证左侧都是小于
    // 此值的数,右侧都是大于此值的数.
    // 在进行递归排序时若得到某个数的最终位置为k-1,则表明数组左侧即为最小的k个数,直接停止递归
    // 若得到某个数的最终位置大于k-1,则向左侧区间递归;若某个数的最终位置大于k-1,则向右侧区间递归
    public ArrayList<Integer> GetLeastNumbers_Solution(int [] array, int k) {
        // 创建返回列表
        ArrayList<Integer> res = new ArrayList<>();
        // 排除特殊情况
        if(array == null || array.length<k) return res;
        // 如果array.length>k,则寻找k个数组最小值,否则不需要递归
        if(array.length > k) getTopK(array,0,array.length-1,k);
        // 遍历数组的前k个元素,将其填充至链表中并返回
        for(int i=0; i<k; i++){
            res.add(array[i]);
        }
        return res;
        
    }
    
    // 用于给数组进行大致排序,直到数组左侧k个值为整体最小
    private void getTopK(int[] array,int begin,int end,int k){
        // 排除特殊情况
        if(begin >= end) return;
        // 定义左右指针
        int left = begin,right = end;
        // 定义哨兵值
        int sensor = array[left];
        while(left<right){
            // 从右侧找到首个小于哨兵的值,将其放置于left处
            while(left<right && array[right]>=sensor) right--;
            array[left] = array[right];
            // 从左侧找到首个大于哨兵的值,将其放置于right处
            while(left<right && array[left]<=sensor) left++;
            array[right] = array[left];
        }
        // 遍历结束后将哨兵放置于升序最终位置
        array[left] = sensor;
        // 如果此位置小于k-1,则向右侧区间递归
        if(left < k-1) getTopK(array,left+1,end,k);
        // 如果此位置大于k-1,则向左侧区间递归
        if(left > k-1) getTopK(array,begin,left-1,k);
    }
}