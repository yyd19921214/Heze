import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) {
        String path="D:\\CloudMusic";
        File f=new File(path);
        if (!f.exists()){
            f.mkdirs();
        }
        f.setReadable(true);

        if (f.canRead())
            System.out.println("yes");
    }
}




