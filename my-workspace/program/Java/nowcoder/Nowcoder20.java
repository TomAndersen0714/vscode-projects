package nowcoder;
import java.util.Stack;
// 定义栈的数据结构，请在该类型中实现一个能够得到栈中所含最小元素的min函数（时间复杂度应为O（1））。
// 注意：保证测试中不会当栈为空的时候，对栈调用pop()或者min()或者top()方法。
public class Nowcoder20{
    // 方法1:使用双栈,其中一个栈用于正常使用,另外一个栈用于保存栈中对应元素到栈底的最小值元素
    private Stack<Integer> dataStack = new Stack<>();
    private Stack<Integer> minStack = new Stack<>();
    
    
    // 将指定元素压入栈中
    public void push(int node) {
        dataStack.push(node);
        // 每次压栈时,判断压栈元素和minStack栈顶元素的大小,将较小者压入minStack
        if(!minStack.isEmpty()){
            minStack.push(Math.min(node,minStack.peek()));
        }else minStack.push(node);
    }
    
    // 获取当前栈顶元素,并且出栈
    public int pop() {
        try{
            minStack.pop();
            return dataStack.pop();
        }catch(Exception e){
            e.printStackTrace();
            return 0;
        }
    }
    
    // 获取当前栈中栈顶元素,但不出栈
    public int top() {
        try{
            return dataStack.peek();
        }catch(Exception e){
            e.printStackTrace();
            return 0;
        }
    }
    
    // 获取当前栈中最小元素
    public int min() {
        try{
            return minStack.peek();
        }catch(Exception e){
            e.printStackTrace();
            return 0;
        }
    }
}