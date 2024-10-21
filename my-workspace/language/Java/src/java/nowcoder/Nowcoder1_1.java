package java.nowcoder;

public class Nowcoder1_1 {
    // 二维矩阵中的每个值都是左上角区域内的最大值
    // 按行从上至下对每行使用二分查找,如果target小于当前行第一个数则直接返回false
    // 如果当前行没找到则进入下一行
    public boolean Find(int target, int [][] array) {
        // 排除特殊情况
        if(array == null || array.length ==0 || array[0].length == 0) return false;
        // 获取行列值
        int row = array.length,col = array[0].length;
        // 从第一行开始遍历
        int i =0;
        while(i < row){
            if(array[i][0] > target) return false;
            int left = 0,right = col -1 ,mid;
            while(left <= right){
                mid = left + (right-left)/2;
                if(array[i][mid] > target) right = mid-1;
                else if(array[i][mid] < target) left = mid+1;
                else return true;
            }
            i++;
        }
        return false;
    }
}