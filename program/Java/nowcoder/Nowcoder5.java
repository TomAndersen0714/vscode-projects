package nowcoder;
//现在要求输入一个整数n，请你输出斐波那契数列的第n项（从0开始，第0项为0）n<=39
public class Nowcoder5{
	public int Fibonacci(int n) {
		if(n==0) return 0;
        if(n==1) return 1;
        int i=2,a=1,b=1,temp;
        while(i!=n){
            temp = b;
            b = a+b;
            a = temp;
            i++;
        }
        return b;
	}
}