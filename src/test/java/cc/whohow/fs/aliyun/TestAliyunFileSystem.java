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
            System.out.println(fs.getPath("/vfs-test/")); // 虚拟文件路径
            fs.list("/vfs-test/").forEach(System.out::println);

            System.out.println(fs.getPath("http://xxx.oss-cn-hangzhou.aliyuncs.com/test/")); // 标准OSS地址
            fs.list("http://xxx.oss-cn-hangzhou.aliyuncs.com/test/").forEach(System.out::println);

            System.out.println(fs.getPath("http://xxx.img-cn-hangzhou.aliyuncs.com/test/")); // CDN地址
            fs.list("http://xxx.img-cn-hangzhou.aliyuncs.com/test/").forEach(System.out::println);
        }
    }
}
