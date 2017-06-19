package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.SimplifiedObjectMeta;

import java.io.*;
import java.net.URI;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AliyunOSSFile implements Comparable<AliyunOSSFile>, Closeable {
    private static final Pattern NAMES = Pattern.compile("(.*?)([^/]+)/?$");
    private final URI uri;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String bucketName;
    private final String endpoint;
    private final String objectKey;
    private volatile OSSClient client;
    private volatile ScheduledFuture<?> watchKey;

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

    public AliyunOSSFile(String accessKeyId, String secretAccessKey, String bucketName, String endpoint, String objectKey) {
        this.uri = URI.create(String.format("http://%s:%s@%s.%s/%s", accessKeyId, secretAccessKey, bucketName, endpoint, objectKey));
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.bucketName = bucketName;
        this.endpoint = endpoint;
        this.objectKey = objectKey;
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

    public boolean delete() {
        getClient().deleteObject(bucketName, objectKey);
        return true;
    }

    public boolean exists() {
        return getClient().doesObjectExist(bucketName, objectKey);
    }

    public String getName() {
        Matcher matcher = NAMES.matcher(objectKey);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        return null;
    }

    public String getParent() {
        Matcher matcher = NAMES.matcher(objectKey);
        if (matcher.matches()) {
            return String.format("http://%s:%s@%s.%s/%s", accessKeyId, secretAccessKey, bucketName, endpoint, matcher.group(1));
        }
        return null;
    }

    public AliyunOSSFile getParentFile() {
        String parent = getParent();
        return parent == null ? null : new AliyunOSSFile(URI.create(parent));
    }

    public boolean isDirectory() {
        return !isFile();
    }

    public boolean isFile() {
        return !objectKey.endsWith("/");
    }

    public SimplifiedObjectMeta getObjectMeta() {
        return getClient().getSimplifiedObjectMeta(bucketName, objectKey);
    }

    public long lastModified() {
        return getObjectMeta().getLastModified().getTime();
    }

    public long length() {
        return getObjectMeta().getSize();
    }

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

    public AliyunOSSFile[] listFiles() {
       return Stream.of(list()).map(URI::create).map(AliyunOSSFile::new).toArray(AliyunOSSFile[]::new);
    }

    public boolean renameTo(String destObjectKey) {
        getClient().copyObject(bucketName, objectKey, bucketName, destObjectKey);
        getClient().deleteObject(bucketName, objectKey);
        return true;
    }

    public URI toURI() {
        return uri;
    }

    public InputStream newInputStream() {
        return getClient().getObject(bucketName, objectKey).getObjectContent();
    }

    public OutputStream newOutputStream() {
        return new AliyunOSSOutputStream(getClient(), bucketName, objectKey);
    }

    public AliyunOSSFileChannel newFileChannel() throws IOException {
        return new AliyunOSSFileChannel(getClient(), bucketName, objectKey);
    }

    public byte[] readAllBytes() throws IOException {
        try (InputStream stream = newInputStream()) {
            return IOUtils.readStreamAsByteArray(stream);
        }
    }

    public String readAllAsString(String charset) throws IOException {
        try (InputStream stream = newInputStream()) {
            return IOUtils.readStreamAsString(stream, charset);
        }
    }

    public synchronized void watch(ScheduledExecutorService executor, long delay, long timeout,
                                   BiFunction<java.nio.file.WatchEvent.Kind<?>, AliyunOSSFile, Boolean> listener) {
        if (watchKey != null) {
            throw new IllegalStateException();
        }
        watchKey = executor.scheduleWithFixedDelay(
                new AliyunOSSFileWatchTask(this, new AliyunOSSFileWatchTask.MaxTime(timeout), listener),
                0, delay, TimeUnit.MILLISECONDS);
    }

    public synchronized void unwatch() {
        if (watchKey == null) {
            throw new IllegalStateException();
        }
        watchKey.cancel(true);
        watchKey = null;
    }

    public void unwatchAndClose() {
        try {
            unwatch();
        } finally {
            close();
        }
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
