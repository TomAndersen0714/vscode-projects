package java.nowcoder;

public  class SortUtil{
    private SortUtil(){}
    
    // 快排(递归):整体升序
    public static void quickSort(int[] array){
        // 排除特殊情况
        if(array == null || array.length <= 1) return;
        // 嵌套调用
        quickSort(array,0,array.length-1);
    }
    
    // 快排(递归):局部升序
    public static void quickSort(int[] array,int begin,int end){
        // 排除特殊情况
        if(array == null || array.length <= 1) return;
        if(begin >= end || begin < 0 || end >= array.length) return;
        // 寻找首个元素的最终位置
        int sentry = array[begin],left = begin,right = end,pos;
        while(left < right){
            while(left < right && array[right] <= sentry) right--;
            array[left] = array[right];
            while(left < right && array[left] >= sentry) left++;
            array[right] = array[left];
        }
        array[left] = sentry;
        pos = left;
        // 递归调用
        quickSort(array,begin,pos-1);
        quickSort(array,pos+1,end);
        
    }
    
    // 快排1(递归):整体升序
    public static void quickSort1(int[] array){
        // 排除特殊情况
        if(array == null || array.length <= 1) return;
        // 嵌套调用
        quickSort1(array,0,array.length-1);
    }
    
    // 快排1(递归):局部升序
    public static void quickSort1(int[] array,int begin,int end){
        // 排除特殊情况
        if(array == null || array.length <= 1) return;
        if(begin >= end || begin < 0 || end >= array.length) return;
        // 寻找首个元素的最终位置
        int pos = partition(array,begin,end);
        // 递归调用
        quickSort1(array,begin,pos-1);
        quickSort1(array,pos+1,end);
    }
    
    // 快排2(非递归):整体升序
    public static void quickSort2(int[] array){
        // 排除特殊情况
        if(array == null || array.length <= 1) return;
        // 嵌套调用
        quickSort2(array,0,array.length-1);
    }
    
    // 快排2(非递归):局部升序(使用辅助栈代替方法栈实现)
    public static void quickSort2(int[] array,int begin,int end){
        // 排除特殊情况
        if(array == null || array.length <= 1) return;
        if(begin >= end || begin < 0 || end >= array.length) return;
        // 使用Stack<Integer>代替方法栈进行迭代
        
    }
    
    // 分区方法:获取数组中首个元素的升序最终位置
    private static int partition(int[] array,int begin,int end){
        return 0;
    }
    
}