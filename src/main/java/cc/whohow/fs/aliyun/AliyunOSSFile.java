package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.SimplifiedObjectMeta;

import java.io.*;
import java.net.URI;
import java.nio.file.WatchEvent;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 轻量级阿里云文件对象，线程不安全，需自行保证client的状态，如不确定，请clone后使用
 */
public class AliyunOSSFile implements Comparable<AliyunOSSFile>, Closeable {
    private static final Pattern NAMES = Pattern.compile("(.*?)([^/]+)/?$");
    private final URI uri; // 标准URI
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String bucketName;
    private final String endpoint;
    private final String objectKey;
    private volatile OSSClient client;

    /**
     * http://[accessKeyId]:[secretAccessKey]@[bucketName].[endpoint]/[objectKey]
     */
    public AliyunOSSFile(URI uri) {
        String[] user = uri.getUserInfo().split(":");
        String[] host = uri.getHost().split("\\.", 2);
        this.uri = uri;
        this.accessKeyId = user[0];
        this.secretAccessKey = user[1];
        this.bucketName = host[0];
        this.endpoint = host[1];
        this.objectKey = uri.getPath().substring(1);
    }

    /**
     * 标准参数
     */
    public AliyunOSSFile(String accessKeyId, String secretAccessKey, String bucketName, String endpoint, String objectKey) {
        this.uri = URI.create(String.format("http://%s:%s@%s.%s/%s", accessKeyId, secretAccessKey, bucketName, endpoint, objectKey));
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.bucketName = bucketName;
        this.endpoint = endpoint;
        this.objectKey = objectKey;
    }

    /**
     * clone
     */
    public AliyunOSSFile(AliyunOSSFile file) {
        this.uri = file.uri;
        this.accessKeyId = file.accessKeyId;
        this.secretAccessKey = file.secretAccessKey;
        this.bucketName = file.bucketName;
        this.endpoint = file.endpoint;
        this.objectKey = file.objectKey;
    }

    @Override
    public int compareTo(AliyunOSSFile o) {
        return this.toString().compareTo(o.toString());
    }

    @Override
    public void close() {
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public OSSClient getClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null) {
                    client = new OSSClient(endpoint, accessKeyId, secretAccessKey);
                }
            }
        }
        return client;
    }

    /**
     * 删除
     */
    public boolean delete() {
        getClient().deleteObject(bucketName, objectKey);
        return true;
    }

    /**
     * 文件是否存在
     */
    public boolean exists() {
        return getClient().doesObjectExist(bucketName, objectKey);
    }

    /**
     * 文件名
     */
    public String getName() {
        Matcher matcher = NAMES.matcher(objectKey);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        return null;
    }

    /**
     * 上级目录
     */
    public String getParent() {
        Matcher matcher = NAMES.matcher(objectKey);
        if (matcher.matches()) {
            return String.format("http://%s:%s@%s.%s/%s", accessKeyId, secretAccessKey, bucketName, endpoint, matcher.group(1));
        }
        return null;
    }

    /**
     * 上级目录
     */
    public AliyunOSSFile getParentFile() {
        String parent = getParent();
        return parent == null ? null : new AliyunOSSFile(URI.create(parent));
    }

    /**
     * 是否是文件夹
     */
    public boolean isDirectory() {
        return !isFile();
    }

    /**
     * 是否是文件
     */
    public boolean isFile() {
        return !objectKey.endsWith("/");
    }

    /**
     * 文件元数据
     */
    public SimplifiedObjectMeta getObjectMeta() {
        return getClient().getSimplifiedObjectMeta(bucketName, objectKey);
    }

    /**
     * 最后修改时间
     */
    public long lastModified() {
        return getObjectMeta().getLastModified().getTime();
    }

    /**
     * 文件长度
     */
    public long length() {
        return getObjectMeta().getSize();
    }

    /**
     * 下级文件列表
     */
    public String[] list() {
        if (isFile()) {
            return null;
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new AliyunOSSObjectSummaryIterator(getClient(), bucketName, objectKey, "/"), 0), false)
                .filter(o -> !o.getKey().equals(objectKey))
                .map(o -> String.format("http://%s:%s@%s.%s/%s", accessKeyId, secretAccessKey, bucketName, endpoint, o.getKey()))
                .toArray(String[]::new);
    }

    /**
     * 下级文件列表
     */
    public AliyunOSSFile[] listFiles() {
       return Stream.of(list()).map(URI::create).map(AliyunOSSFile::new).toArray(AliyunOSSFile[]::new);
    }

    /**
     * 重命名
     */
    public boolean renameTo(String destObjectKey) {
        getClient().copyObject(bucketName, objectKey, bucketName, destObjectKey);
        getClient().deleteObject(bucketName, objectKey);
        return true;
    }

    public URI toURI() {
        return uri;
    }

    /**
     * 读
     */
    public InputStream newInputStream() {
        return getClient().getObject(bucketName, objectKey).getObjectContent();
    }

    /**
     * 写，文件需不存在，默认 128KB 缓冲区
     */
    public OutputStream newOutputStream() {
        return new BufferedOutputStream(new AliyunOSSOutputStream(getClient(), bucketName, objectKey), 128 * 1024);
    }

    /**
     * 读写
     */
    public AliyunOSSFileChannel newFileChannel() throws IOException {
        return new AliyunOSSFileChannel(getClient(), bucketName, objectKey);
    }

    /**
     * 读取所有字节
     */
    public byte[] readAllBytes() throws IOException {
        try (InputStream stream = newInputStream()) {
            return IOUtils.readStreamAsByteArray(stream);
        }
    }

    /**
     * 读取所有文本
     */
    public String readAllAsString(String charset) throws IOException {
        try (InputStream stream = newInputStream()) {
            return IOUtils.readStreamAsString(stream, charset);
        }
    }

    /**
     * 监视文件
     */
    public AliyunOSSFileWatcher watch(BiFunction<WatchEvent.Kind<?>, AliyunOSSFileWatcher, Boolean> listener) {
        if (!isFile()) {
            throw new UnsupportedOperationException();
        }
        return new AliyunOSSFileWatcher(this, listener);
    }

    public String toString() {
        return String.format("http://%s.%s/%s", bucketName, endpoint, objectKey);
    }

    @Override
    public boolean equals(Object object) {
        AliyunOSSFile that = (AliyunOSSFile) object;
        return Objects.equals(that.bucketName, this.bucketName) && Objects.equals(that.objectKey, this.objectKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketName, objectKey);
    }
}
