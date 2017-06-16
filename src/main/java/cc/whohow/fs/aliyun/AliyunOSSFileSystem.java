package cc.whohow.fs.aliyun;

import cc.whohow.fs.PatternPathMatcher;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 阿里云文件系统
 */
public class AliyunOSSFileSystem extends FileSystem {
    private final AliyunOSSFileSystemProvider fileSystemProvider;
    private final Properties properties;
    private final AliyunOSSFileStore fileStore; // 文件系统所在Bucket

    private final String vfs; // 虚拟文件系统挂载点
    private final String prefix; // OSS前缀
    private final URI uri; // 标准URI，http://BucketName.ExtranetEndpoint/Prefix

    public AliyunOSSFileSystem(AliyunOSSFileSystemProvider fileSystemProvider, Properties properties) {
        this.fileSystemProvider = fileSystemProvider;
        this.properties = properties;
        this.fileStore = fileSystemProvider.getFileStore(properties);
        this.vfs = properties.getProperty("vfs");
        this.prefix = properties.getProperty("prefix", "");
        this.uri = fileStore.getUri().resolve("/" + prefix);
    }

    public AliyunOSSFileStore getFileStore() {
        return fileStore;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getVfs() {
        return vfs;
    }

    public String getPrefix() {
        return prefix;
    }

    public URI getUri() {
        return uri;
    }

    /**
     * 文件系统所有可能URI
     */
    public List<String> getAlternativeUris() {
        List<String> list = fileStore.getCname().stream()
                .map(self -> "http://" + self + "/" + prefix)
                .collect(Collectors.toList());
        list.add(vfs);
        return list;
    }


    @Override
    public AliyunOSSFileSystemProvider provider() {
        return fileSystemProvider;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return fileStore.isReadOnly();
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    /**
     * 获取跟路径下所有文件夹
     */
    @Override
    public Iterable<Path> getRootDirectories() {
        return () -> StreamSupport.stream(fileSystemProvider.list(getPath("/")).spliterator(), false)
                .map(self -> (Path) new AliyunOSSPath(this, self))
                .iterator();
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return Collections.singleton(fileStore);
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return properties.stringPropertyNames();
    }

    /**
     * 根据字符串生成路径
     */
    @Override
    public AliyunOSSPath getPath(String first, String... more) {
        StringBuilder buffer = new StringBuilder();
        if (first.startsWith("/")) {
            // 绝对路径
            buffer.append("/");
            buffer.append(prefix);
            buffer.append(first, 1, first.length());
        } else {
            // 相对路径
            buffer.append(first);
        }
        for (String s : more) {
            buffer.append('/').append(s);
        }
        return new AliyunOSSPath(this, buffer.toString());
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        return new PatternPathMatcher(syntaxAndPattern);
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService() throws IOException {
        return fileSystemProvider.getWatchService();
    }

    @Override
    public String toString() {
        return getUri().toString();
    }
}
