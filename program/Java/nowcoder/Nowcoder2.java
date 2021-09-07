package nowcoder;
public class Nowcoder2{
    // 直接使用StringBuffer类自带的replace方法
    public String replaceSpace(StringBuffer str) {
        // 排除意外情况
        if(str == null ) return null;
        // 进行正常遍历
        for(int i=0;i < str.length();i++){
            if(str.charAt(i) == ' ') str.replace(i,i+1,"%20");
        }
        return str.toString();
    }
}