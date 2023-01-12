package nowcoder;
import java.util.ArrayList;
// 输入n个整数，找出其中最小的K个数。例如输入4,5,1,6,2,7,3,8这8个数字，则最小的4个数字是1,2,3,4。
// 注意:当没有k个数时,返回空列表
public class Nowcoder29_1{
    // 方法2:使用最小堆排序
    public ArrayList<Integer> GetLeastNumbers_Solution(int [] array, int k) {
        // 创建返回列表
        ArrayList<Integer> res = new ArrayList<>();
        // 排除特殊情况
        if(array == null || array.length == 0 || array.length < k) return res;
        // 如果array.length > k则进行最小堆排序
        if(array.length > k){
            for(int i=array.length/2; i>=0; i--){
                adjustMinHeap(array,0,array.length-1,i);
            }
            int count = k,i = array.length - 1;
            while(count-->0){
                swap(array,0,i);
                adjustMinHeap(array,0,i-1,0);
                i--;
            }
        }
        // 获取数组最后k个元素,即最小k个元素
        for(int i=array.length-1; i>=array.length - k; i--){
            res.add(array[i]);
        }
        // 返回结果列表
        return res;
    }
    
    // 调整最小堆
    private void adjustMinHeap(int[] array,int begin,int end,int i){
        // 排除特殊情况
        if(i >= end) return;
        // 定义子节点位置,保存父节点值
        int k = begin + (i - begin)*2 + 1,sensor = array[i];
        // 循环调整最小堆
        while(k <= end){
            // 取子节点中较小者
            if(k+1 <= end && array[k+1] < array[k]) k++;
            // 如果子节点较小者小于父节点,则将其放置于父节点位置
            // 否则,表明当前调整已经符合最小堆,退出循环
            if(array[k] < sensor) array[i] = array[k];
            else break;
            // 更新下一次比较的位置
            i = k;
            k = begin + (i - begin)*2 + 1;
        }
        // 循环结束时,将父节点值放置在最终位置
        array[i] = sensor;
    }
    
    // 交换数组中指定元素
    private void swap(int[] array, int a,int b){
        int temp = array[a];
        array[a] = array[b];
        array[b] = temp;
    }
}