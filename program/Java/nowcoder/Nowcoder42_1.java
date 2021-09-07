package nowcoder;

import java.util.ArrayList;

// 输入一个递增排序的数组和一个数字S，在数组中查找两个数，使得他们的和正好是S，
// 如果有多对数字的和等于S，输出两个数的乘积最小的。
// 注意:对应每个测试案例，输出两个数，小的先输出。
public class Nowcoder42_1{
    // 方法2:双指针
    // 思路:初始时双指针分别指向数组首尾,当双指针元素之和小于sum时,则左指针右移
    // 当双指针元素之和大于sum时,则右指针左移,当双指针元素之和等于sum时,直接将
    // 两个元素添加到返回列表,然后返回即可,因为窗口越大,双指针对应元素乘积越小
    public ArrayList<Integer> FindNumbersWithSum(int [] array,int sum) {
        // 创建返回列表
        ArrayList<Integer> res = new ArrayList<>();
        // 排除特殊情况
        if(array == null || array.length <= 1) return res;
        // 定义双指针
        int left = 0,right = array.length - 1;
        while(left < right){
            if((array[left]+array[right])<sum) left++;
            else if((array[left]+array[right])>sum) right--;
            else{
                res.add(array[left]);
                res.add(array[right]);
                return res;
            }
        }
        // 返回结果
        return res;
    }
}