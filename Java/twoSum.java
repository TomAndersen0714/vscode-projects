
import java.util.Arrays;
import java.util.HashMap;

// VScode用作Java初学者练习不好使
public class twoSum {
    public static int[] _twoSum(int[] nums, int target) {
        int[] indexs = { 0, 0 };
        HashMap<Integer, Integer> myMap = new HashMap<>();
        for (int i = 0; i < nums.length; i++) {
            if (myMap.containsKey(target - nums[i])) {
                indexs[0] = myMap.get(target - nums[i]);
                indexs[1] = i;
                return indexs;
            }
            myMap.put(nums[i], i);
        }
        return indexs;
    }

    public static void main(String[] args) {
        int[] nums = { -3, 4, 3, 90 };
        int target = 0;
        System.out.println(Arrays.toString(_twoSum(nums, target)));
    }
}