package nowcoder;
// 给定一个数组A[0,1,...,n-1],请构建一个数组B[0,1,...,n-1],其中B中的元素
// B[i]=A[0]*A[1]*...*A[i-1]*A[i+1]*...*A[n-1]。不能使用除法。
// （注意：规定B[0] = A[1] * A[2] * ... * A[n-1]，B[n-1] = A[0] * A[1] * ... * A[n-2];）
import java.util.Arrays;
public class Nowcoder51_1{
    // 方法2:正反两次遍历数组
    // 思路:由于结果数组中每个元素的值由输入数组的两部分的乘积组成,因此可以
    // 分两次遍历输入数组,两次从不同方向遍历,首次遍历时正向遍历求得部分值,
    // 二次遍历时,逆向遍历求得最终值.
    // 最终即可求得结果数组中每个元素的值.
    public int[] multiply(int[] A) {
        // 排除特殊情况
        if(A == null || A.length == 0) return null;
        // 声明返回数组
        int[] res = new int[A.length];
        // 正向遍历输入数组,求得部分值
        res[0] = 1;
        for(int i=1; i<A.length; i++){
            res[i] = res[i-1]*A[i-1];
        }
        // 逆向遍历输入数组,求得最终值
        int temp = 1;
        for(int j=A.length-1; j>=0; j--){
            res[j] *= temp;
            temp *= A[j];
        }
        // 返回结果数组
        return res;
    }
}