package java.nowcoder;
import java.util.ArrayList;
    // 输入一个矩阵，按照从外向里以顺时针的顺序依次打印出每一个数字，
    // 例如，如果输入如下4 X 4矩阵： 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 
    // 则依次打印出数字1,2,3,4,8,12,16,15,14,13,9,5,6,7,11,10.
public class Nowcoder19_1{
    // 方法2:改进自方法1,不使用方向数组,也不使用标记矩阵
    // 通过直接限定遍历方向,代替方向数组
    // 通过改变边界值来限制遍历的范围,代替标记矩阵
    public ArrayList<Integer> printMatrix(int [][] matrix) {
        // 创建返回列表
        ArrayList<Integer> printList = new ArrayList<>();
        // 排除特殊情况
        if(matrix == null || matrix.length == 0 || matrix[0].length == 0)
            return printList;
        // 定义遍历时的上下左右边界,每次遍历时判断是否到达边界
        // 每次遍历结束后,修改边界条件
        int row = matrix.length, col = matrix[0].length;
        int left = 0, right = col-1, top = 0, bottom = row-1;
        // 开始遍历
        while(left<=right && top<=bottom){
            // 向右遍历到底
            for(int i=left; i<=right; i++){
                printList.add(matrix[top][i]);
            }
            // 向下遍历到底(注意交点的处理)
            for(int i=top+1; i<=bottom; i++){
                printList.add(matrix[i][right]);
            }
            // 如果上下边界不重叠,则向左遍历到底
            if(top != bottom){
                for(int i=right-1; i>=left; i--){
                    printList.add(matrix[bottom][i]);
                }
            }
            // 如果左右边界不重叠,则向上遍历到底(注意交点的处理)
            if(left != right){
                for(int i=bottom-1; i>top; i--){
                    printList.add(matrix[i][left]);
                }
            }
            left++;right--;top++;bottom--;
        }
        return printList;
    }
}