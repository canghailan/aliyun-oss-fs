package cc.whohow.fs.aliyun;

import java.io.InputStream;
import java.util.Properties;

public class TestAliyunFileSystem {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try (InputStream props = Thread.currentThread().getContextClassLoader().getResourceAsStream("aliyun-oss.properties")) {
            properties.load(props);
        }
        try (AliyunOSSFileSystemProvider fs = new AliyunOSSFileSystemProvider(properties)) {
           fs.getPath("/temp/test.txt").watch((e) -> {
               System.out.println("test");
               System.out.println(fs.getWatchService());
               System.out.println(e.kind());
               System.out.println(e.getTargetUri());
               System.out.println("\n\n\n");
               return true;
           });
            fs.getPath("/temp/test1.txt").watch((e) -> {
                System.out.println("test1");
                System.out.println(fs.getWatchService());
                System.out.println(e.kind());
                System.out.println(e.getTargetUri());
                System.out.println("\n\n\n");
                return true;
            });
           Thread.sleep(3600000);
        }
    }
}
