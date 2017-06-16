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
            AliyunOSSPath path =  fs.getPath("/temp/");
            path.watch((e) -> {
                System.out.println(e.kind());
                System.out.println(e.getBucket());
                System.out.println(e.getObjectKey());
                return null;
            });
            Thread.sleep(10000000);
        }
    }
}
