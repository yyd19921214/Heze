import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) {

        Map<String,String> m1=new HashMap<>();
        Map<String,String> m2=new HashMap<>();

        m1.put("hello","world");
        m1.put("good","morning");
        m1.put("hi","boy");

        m2.put("hello","world");
        m2.put("hi","boy");
        m2.put("good","morning");

        System.out.println(m1.toString());
        System.out.println(m2.toString());
        System.out.println(m1.equals(m2));
    }
}




