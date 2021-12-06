abstract class AbstractClass{
    AbstractClass(){};
    abstract void methodA();
}

public class AbstractClassTest extends AbstractClass{
    private static int a = 1;
    void methodA(){}
    public static void main(String[] args){
        System.out.println(AbstractClassTest.a);
    }
}