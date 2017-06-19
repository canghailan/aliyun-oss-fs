package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObjectSummary;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 阿里云OSS文件存储：Bucket
 */
public class AliyunOSSFileStore extends FileStore {
    private final AliyunOSSFileSystemProvider fileSystemProvider;
    private final Properties properties;
    private final OSSClient client; // 阿里云OSS客户端

    private final String bucketName;
    private final String extranetEndpoint; // 外网接入点
    private final String intranetEndpoint; // 内网接入点
    private final URI uri; // 标准URI，http://BucketName.ExtranetEndpoint
    private final List<String> cname; // 关联域名：CDN，外网，内网等，按优先级排序

    public AliyunOSSFileStore(AliyunOSSFileSystemProvider fileSystemProvider, Properties properties) {
        String accessKeyId = properties.getProperty("accessKeyId");
        String secretAccessKey = properties.getProperty("secretAccessKey");
        List<String> cname = Arrays.stream(properties.getProperty("cname", "").split(","))
                .map(String::trim)
                .filter(self -> !self.isEmpty())
                .collect(Collectors.toList());
        this.bucketName = properties.getProperty("bucketName");
        this.extranetEndpoint = properties.getProperty("endpoint");
        this.intranetEndpoint = properties.getProperty("endpoint-internal");

        this.fileSystemProvider = fileSystemProvider;
        this.properties = properties;
        this.client = fileSystemProvider.getOSSClient(accessKeyId, secretAccessKey,
                fileSystemProvider.isIntranet() ? intranetEndpoint : extranetEndpoint); // 获取共享OSSClient，优先内网
        this.uri = URI.create("http://" + bucketName + "." + extranetEndpoint);

        cname.add(bucketName + "." + extranetEndpoint);
        cname.add(bucketName + "." + intranetEndpoint);
        this.cname = cname.stream().distinct().collect(Collectors.toList());
    }

    public AliyunOSSFileSystemProvider provider() {
        return fileSystemProvider;
    }

    public String getScheme() {
        return uri.getScheme();
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getExtranetEndpoint() {
        return extranetEndpoint;
    }

    public String getIntranetEndpoint() {
        return intranetEndpoint;
    }

    public URI getUri() {
        return uri;
    }

    public OSSClient getOSSClient() {
        return client;
    }

    public List<String> getCname() {
        return cname;
    }

    @Override
    public String name() {
        return bucketName + "." + extranetEndpoint;
    }

    @Override
    public String type() {
        return "AliyunOSSBucket";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    /**
     * 总空间
     */
    @Override
    public long getTotalSpace() throws IOException {
//        return client.getBucketStorageCapacity(bucketName).getStorageCapacity() * 1024L * 1024L * 1024L;
        return Long.MAX_VALUE;
    }

    /**
     * 已使用空间，字节
     */
    public long getUsedSpace() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new AliyunOSSObjectSummaryIterator(client, bucketName, null), 0), true)
                .mapToLong(OSSObjectSummary::getSize)
                .sum();
    }

    @Override
    public long getUsableSpace() throws IOException {
        return getTotalSpace() - getUsedSpace();
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        return getUsableSpace();
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return false;
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
        return false;
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        return properties.getProperty(attribute);
    }

    @Override
    public String toString() {
        return getUri().toString();
    }
}
