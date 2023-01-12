package nowcoder;
import java.util.Stack;
public class Nowcoder7{
	// 用两个栈来实现一个队列，完成队列的Push和Pop操作。 队列中的元素为int类型。
	Stack<Integer> inStack = new Stack<>();
	Stack<Integer> outStack = new Stack<>();
	
	// 出队列方法
	public int pop(int node){
		if(outStack.isEmpty()){
			while(!inStack.isEmpty()){
				outStack.push(inStack.pop());
			}
		}
		return outStack.pop();
	}
	
	// 入队方法
	public void push(int node){
		inStack.push(node);
	}
}