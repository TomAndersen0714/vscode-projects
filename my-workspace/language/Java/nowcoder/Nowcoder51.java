package nowcoder;
// 给定一个数组A[0,1,...,n-1],请构建一个数组B[0,1,...,n-1],其中B中的元素
// B[i]=A[0]*A[1]*...*A[i-1]*A[i+1]*...*A[n-1]。不能使用除法。
// （注意：规定B[0] = A[1] * A[2] * ... * A[n-1]，B[n-1] = A[0] * A[1] * ... * A[n-2];）
import java.util.Arrays;
public class Nowcoder51{
    // 方法1:使用两个辅助数组
    // 思路:创建两个数组left/right分别保存A[0]*A[1]*...*A[i]和A[i]*...*A[n-1],遍历输入数组A,
    // 填充数组left/right.然后遍历辅助数组,填充返回数组.
    // 时间复杂度:O(n),空间复杂度:O(n)
    public int[] multiply(int[] A) {
        // 排除特殊情况
        if(A == null || A.length == 0) return null;
        // 创建辅助数组,设置边界条件
        int[] res = new int[A.length];
        int[] left = new int[A.length];
        int[] right = new int[A.length];
        left[0] = A[0];
        right[A.length -1] = A[A.length -1];
        // 遍历输入数组,填充辅助数组
        for(int i=1,j=A.length-2; i<A.length; i++){
            j = A.length - i - 1;
            left[i] = left[i-1]*A[i];
            right[j] = right[j+1] * A[j];
        }
        // 遍历辅助数组,填充返回数组
        res[0] = right[1];
        res[A.length - 1] = left[A.length - 2];
        for(int i=1; i<A.length -1; i++){
            res[i] = left[i-1]*right[i+1];
        }
        // 返回结果数组
        return res;
    }
}