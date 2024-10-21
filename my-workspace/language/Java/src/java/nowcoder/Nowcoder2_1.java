package java.nowcoder;
public class Nowcoder2_1{
    // 遍历StringBuffer,并使用StringBuilder保存转换后和不需要转换的字符
    public String replaceSpace(StringBuffer str) {
        // 排除特殊情况
        if(str == null) return null;
        // 进行正常遍历
        StringBuilder strBuilder = new StringBuilder();
        int len = str.length();
        for(int i = 0 ; i < len ; i++){
            if(str.charAt(i)==' ') strBuilder.append("%20");
            else strBuilder.append(str.charAt(i));
        }
        return strBuilder.toString();
    }
}