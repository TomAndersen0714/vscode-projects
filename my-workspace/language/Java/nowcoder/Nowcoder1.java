package nowcoder;

public class Nowcoder1 {
    // 二维矩阵中的每个值都是左上角区域内的最大值
    // 从左下角开始遍历,当前值大于target时向上查找,当前值小target时向右查找
    public boolean Find(int target, int [][] array) {
        // 排除特殊情况
        if(array == null || array.length == 0) return false;
        // 获取行列数
        int row = array.length,col = array[0].length;
        // 定义查找开始位置-左下角
        int i = row - 1, j = 0;
        while(i>=0 && j<col){
            if(array[i][j] < target) j++;
            else if(array[i][j] > target) i--;
            else return true;
        }
        return false;
    }
}