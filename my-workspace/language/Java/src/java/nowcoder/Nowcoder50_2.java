package java.nowcoder;
// 在一个长度为n的数组里的所有数字都在0到n-1的范围内。 
// 数组中某些数字是重复的，但不知道有几个数字是重复的。也不知道每个数字重复几次。
// 请找出数组中任意一个重复的数字。 
// 例如，如果输入长度为7的数组{2,3,1,0,2,5,3}，那么对应的输出是第一个重复的数字2。
public class Nowcoder50_2{
    // 方法3:利用题目提供的特殊条件
    // 思路:因为题目中说到长度为n的数组中的数所在范围为0~n-1,因此如果此数组中没有
    // 重复数字,那么每个数字都可以存放在与其数值相同的位置上,即数字0在位置0上,数字1
    // 在位置1上等等.
    // 因此遍历数组,若当前数字已经在对应位置上,则跳过当前数字,否则,继续判断,若当前
    // 数字的对应位置上的数字与当前数字不同,则将对应位置上数字与当前数字进行交换,即:
    // temp = numbers[numbers[i]],numbers[numbers[i]] = numbers[i],numbers[i] = temp;
    // 否则,则表明在交换之前对应位置上已经存在相同数,即numbers[numbers[i]] == numbers[i]
    // 则将当前数字添加到返回数组中,并返回true.遍历结束时,返回false.
    // 时间复杂度:O(n),空间复杂度:O(1)
    public boolean duplicate(int numbers[],int length,int [] duplication) {
        // 排除特殊情况
        if(numbers == null || numbers.length != length || numbers.length <=1)
            return false;
        // 遍历数组,尝试将数字归位
        int temp;
        for(int i = 0; i < length;){
            // 如果当前数字已经在对应位置上,则跳过当前数字
            if(numbers[i] == i){
                i++;
                continue;
            }
            // 如果当前数字不在对应位置上,且其对应位置上的数字与当前数字不同,则进行交换,但i不变
            if(numbers[numbers[i]] != numbers[i]){
                temp = numbers[numbers[i]];
                numbers[numbers[i]] = numbers[i];
                numbers[i] = temp;
            }
            // 如果当前数字不在对应位置上,但其对应位置上已经有相同数字,
            // 则将此数字添加到返回数组中,并返回true
            else{
                duplication[0] = numbers[i];
                return true;
            }
        }
        // 遍历结束,所有数字归位,则表明没有重复数字
        return false;
    }
}