package nowcoder;
import java.util.ArrayList;
// 输入n个整数，找出其中最小的K个数。例如输入4,5,1,6,2,7,3,8这8个数字，则最小的4个数字是1,2,3,4。
// 注意:当没有k个数时,返回空列表
public class Nowcoder29_2{
    // 方法3:使用冒泡排序
    // 当k<array.length时,遍历数组k次,将k个最小值冒泡出来,最后遍历数组的k个元素,添加到列表中返回
    public ArrayList<Integer> GetLeastNumbers_Solution(int [] array, int k) {
        // 创建返回列表
        ArrayList<Integer> res = new ArrayList<>();
        // 排除特殊情况
        if(array == null || array.length < k) return res;
        // 如果array.length>k,则进行冒泡排序
        if(array.length > k)  getTopK(array,k);
        // 遍历数组获取TopK元素
        for(int i=0; i<k; i++){
            res.add(array[i]);
        }
        return res;
        
    }
    
    // 通过冒泡排序的方式给TopK个元素排序,即冒泡k次
    private void getTopK(int[] array,int k){
        int temp;
        while(k-- > 0){
            for(int i=array.length-1; i>0; i--){
                if(array[i] < array[i-1]){
                    temp = array[i];
                    array[i] = array[i-1];
                    array[i-1] = temp;
                }
            }
        }
    }
}