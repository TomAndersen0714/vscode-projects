package nowcoder;
public class Nowcoder13{
	// 输入一个整数数组，实现一个函数来调整该数组中数字的顺序，	
	// 使得所有的奇数位于数组的前半部分，所有的偶数位于数组的后半部分，
	// 并保证奇数和奇数，偶数和偶数之间的相对位置不变。
	public void reOrderArray(int [] array) {
		// 排除特殊情况
		if(array == null || array.length == 1) return;
		// 一般情况
		int temp;
		for(int i=1; i<array.length; i++){
			// 如果当前数为奇数则向左冒泡
			if((array[i]&1) == 1){
				for(int j=i; j>=1; j--){
					if((array[j-1]&1) == 0){
						temp = array[j-1];
						array[j-1] = array[j];
						array[j] = temp;
					}
				}
			}
		}
	}
}