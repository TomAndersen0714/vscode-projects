package nowcoder;
import java.util.Stack;
// 输入两个整数序列，第一个序列表示栈的压入顺序，请判断第二个序列是否可能为该栈的弹出顺序。
// 假设压入栈的所有数字均不相等。例如序列1,2,3,4,5是某栈的压入顺序，
// 序列4,5,3,2,1是该压栈序列对应的一个弹出序列，但4,3,5,1,2就不可能是该压栈序列的弹出序列。
//（注意：这两个序列的长度是相等的）
public class Nowcoder21{
    // 方法1:使用Stack来模拟规定的入栈和出栈的行为
    public boolean IsPopOrder(int [] pushA,int [] popA) {
        // 排除特殊情况
        if(pushA == null || popA == null || pushA.length != popA.length)
            return false;
        // 创建栈用于模拟规定的出入栈顺序
        Stack<Integer> stack = new Stack<>();
        // 定义指针指向下一个出栈元素
        int cursor = 0, len = pushA.length;
        // 依次入栈,并尝试出栈
        for(int i=0; i<len; i++){
            stack.push(pushA[i]);
            while(!stack.isEmpty() && stack.peek() == popA[cursor]){
                stack.pop();
                cursor++;
            }
        }
        return cursor==len;
    }
}