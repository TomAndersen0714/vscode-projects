
class C {
    C() {
        System.out.print("C");
    }
}

class A {
    C c = new C();

    A() {
        this("A");
        System.out.print("A");
    }

    A(String s) {
        System.out.print(s);
    }
}

class Test extends A {
    Test() {
        super("B");
        System.out.print("B");
    }

    public static void main(String[] args) {
        new Test();
        // byte a = 127, b = Byte.MIN_VALUE;
        // a+=1;
        // b-=1;
        // System.out.println(a);
        // System.out.println(b);
        
        // System.out.println(Math.round(1.5d));
        // System.out.println(Math.round(-1.5d));
        // System.out.println(Math.ceil(1.5d));
        // System.out.println(Math.ceil(-1.5d));
        // System.out.println(Math.floor(1.5d));
        // System.out.println(Math.floor(-1.5d));
    }
}