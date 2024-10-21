package java.nowcoder;
// 统计一个数字在排序(升序)数组中出现的次数。
public class Nowcoder37{
	// 方法1:二分查找
	// 思路:因为原数组已经升序排序,因此使用二分法可以确定指定的数在数组中的位置.
	// 找到对应元素在数组中的位置之后,将其向两边扩展并统计出现次数,最后返回即可.
	public int GetNumberOfK(int [] array , int k) {
		// 排除特殊情况
		if(array == null || array.length == 0) return 0;
		// 使用二分查找确定元素所在位置,如果不存在则直接返回0次
		int index = binarySearch(array,0,array.length - 1,k);
		if(index == -1) return 0;
		// 如果找到了,则向两边扩展,获取所有相同元素的个数
		int count = 1, i=index-1, j=index+1;
		while(i>=0){
			if(array[i--] == k) count++;
			else break;
		}
		while(j<array.length){
			if(array[j++] == k) count++;
			else break;
		}
		// 返回出现次数
		return count;
	}
	
	// 升序数组的二分查找算法
	private int binarySearch(int[] array, int begin, int end, int num){
		int left = begin, right = end, mid;
		while(left <= right){
			mid = left + (right - left)/2;
			if(array[mid] > num){
				right = mid -1;
			}else if(array[mid] < num){
				left = mid + 1;
			}else return mid;
		}
		// 若没找到,则返回-1
		return -1;
	}
}