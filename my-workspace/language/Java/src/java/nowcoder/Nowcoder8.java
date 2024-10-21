package java.nowcoder;
public class Nowcoder8{
	// 使用二分查找,每次保存上一次的查找值,通过比较中间与最左端值的大小
    // 来判断下一次的查找方向
	public int minNumberInRotateArray(int [] array) {
		// 排除意外情况
		if(array == null || array.length == 0) return 0;
		if(array.length == 1) return array[0];
		int left = 0,right = array.length - 1,mid;
		while(left < right){
			mid = left + (right - left)/2;
			if(array[mid] > array[left]) left = mid;
			else if(array[mid] < array[left]) right = mid;
			else left++;
		}
		return array[left];
	}
}