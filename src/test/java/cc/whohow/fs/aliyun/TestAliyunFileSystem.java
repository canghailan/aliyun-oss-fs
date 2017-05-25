package cc.whohow.fs.aliyun;

import com.aliyun.oss.model.ObjectMetadata;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestAliyunFileSystem {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        try (InputStream props = Thread.currentThread().getContextClassLoader().getResourceAsStream("aliyun-oss.properties")) {
            properties.load(props);
        }
        try (AliyunOSSFileSystemProvider fs = new AliyunOSSFileSystemProvider(properties)) {
            AliyunOSSPath path =  fs.getPath("/vfs-test/test.txt");
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setLastModified(new Date());
            Map<String, String> map = new HashMap<>();
            map.put("content-sha256", "aa");
            map.put("content-md5", "bb");
            objectMetadata.setUserMetadata(map);
            fs.setMetadata(path, objectMetadata);
            System.out.println(fs.getContentAsString(path, "utf-8"));
            System.out.println(fs.getMetadata(path).getLastModified());
            System.out.println(fs.getMetadata(path).getUserMetadata());

            try (SeekableByteChannel channel = fs.newByteChannel(path, null, null)) {
                channel.truncate(0);
                channel.write(ByteBuffer.wrap("new".getBytes()));
            }
            System.out.println(fs.getContentAsString(path, "utf-8"));
        }
    }
}
