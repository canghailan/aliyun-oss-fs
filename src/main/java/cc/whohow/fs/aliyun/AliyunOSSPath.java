package cc.whohow.fs.aliyun;

import cc.whohow.fs.Names;
import com.aliyun.oss.OSSClient;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.*;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * OSS路径。以/开头为绝对路径，否则为相对路径。以/结尾为目录，否则为普通文件。
 */
public class AliyunOSSPath implements Path {
    private final AliyunOSSFileSystem fileSystem; // 所属文件系统
    private final String pathDescriptor; // 路径描述符，绝对路径为 /ObjectKey，相对路径为name1/name2/.../nameN

    public AliyunOSSPath(AliyunOSSFileSystem fileSystem, String pathDescriptor) {
        this.fileSystem = fileSystem;
        this.pathDescriptor = pathDescriptor;
    }

    public AliyunOSSPath(AliyunOSSPath parent, String child) {
        this.fileSystem = parent.fileSystem;
        this.pathDescriptor = parent.pathDescriptor + child;
    }

    public OSSClient getClient() {
        return getFileStore().getClient();
    }

    public String getBucketName() {
        return getFileStore().getBucketName();
    }

    /**
     * 路径描述符
     */
    public String getPathDescriptor() {
        return pathDescriptor;
    }

    /**
     * 虚拟文件系统路径，无法获取返回null
     */
    public String getVfsPath() {
        if (pathDescriptor.startsWith("/") && pathDescriptor.startsWith(fileSystem.getPrefix(), 1)) {
            return fileSystem.getVfs() + pathDescriptor.substring(fileSystem.getPrefix().length() + 1);
        }
        return null;
    }

    /**
     * OSS ObjectKey，无法获取返回null
     */
    public String getObjectKey() {
        return pathDescriptor.startsWith("/") ? pathDescriptor.substring(1) : null;
    }

    public AliyunOSSFileStore getFileStore() {
        return fileSystem.getFileStore();
    }

    @Override
    public AliyunOSSFileSystem getFileSystem() {
        return fileSystem;
    }

    public boolean isFile() {
        return !pathDescriptor.endsWith("/");
    }

    public boolean isDirectory() {
        return pathDescriptor.endsWith("/");
    }

    @Override
    public boolean isAbsolute() {
        return pathDescriptor.startsWith("/");
    }

    @Override
    public AliyunOSSPath getRoot() {
        return new AliyunOSSPath(fileSystem, "/" + fileSystem.getPrefix());
    }

    /**
     * 文件名
     */
    @Override
    public AliyunOSSPath getFileName() {
        return new AliyunOSSPath(fileSystem, Names.getName(pathDescriptor));
    }

    /**
     * 上级目录
     */
    @Override
    public AliyunOSSPath getParent() {
        String parent = Names.getPrefix(pathDescriptor);
        if (parent.isEmpty()) {
            return null;
        }
        return new AliyunOSSPath(fileSystem, parent);
    }

    public Iterable<String> getNames() {
        return Names.getNames(toString());
    }

    @Override
    public int getNameCount() {
        return Names.getNameCount(toString());
    }

    @Override
    public AliyunOSSPath getName(int index) {
        return StreamSupport.stream(getNames().spliterator(), false)
                .skip(index)
                .findFirst()
                .map(name -> new AliyunOSSPath(fileSystem, name))
                .orElse(null);
    }

    @Override
    public AliyunOSSPath subpath(int beginIndex, int endIndex) {
        String names = StreamSupport.stream(getNames().spliterator(), false)
                .skip(beginIndex)
                .limit(endIndex - beginIndex + 1)
                .collect(Collectors.joining("/"));
        return names.isEmpty() ? null : new AliyunOSSPath(fileSystem, names);
    }

    @Override
    public boolean startsWith(Path other) {
        return startsWith(other.toString());
    }

    @Override
    public boolean startsWith(String other) {
        return toString().startsWith(other);
    }

    @Override
    public boolean endsWith(Path other) {
        return endsWith(other.toString());
    }

    @Override
    public boolean endsWith(String other) {
        return toString().endsWith(other);
    }

    @Override
    public AliyunOSSPath normalize() {
        return new AliyunOSSPath(fileSystem, URI.create(pathDescriptor).normalize().toString());
    }

    @Override
    public AliyunOSSPath resolve(Path other) {
        return resolve(other.toString());
    }

    @Override
    public AliyunOSSPath resolve(String other) {
        return new AliyunOSSPath(fileSystem, URI.create(pathDescriptor).resolve(other).toString());
    }

    @Override
    public AliyunOSSPath resolveSibling(Path other) {
        return getParent().resolve(other);
    }

    @Override
    public AliyunOSSPath resolveSibling(String other) {
        return getParent().resolve(other);
    }

    @Override
    public AliyunOSSPath relativize(Path other) {
        return new AliyunOSSPath(fileSystem, URI.create(toString()).relativize(URI.create(other.toString())).toString());
    }

    /**
     * OSS标准URI
     */
    @Override
    public URI toUri() {
        if (isAbsolute()) {
            return getFileStore().getUri().resolve(pathDescriptor);
        }
        return null;
    }

    /**
     * 最优URL，一般为CDN地址
     */
    public URL toUrl() {
        try {
            return new URL(getFileStore().getScheme() + "://" + getFileStore().getCname().get(0) + pathDescriptor);
        } catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public AliyunOSSPath toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public AliyunOSSPath toRealPath(LinkOption... options) throws IOException {
        return this;
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        return register(watcher, events);
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events) throws IOException {
        if (fileSystem.provider().getWatchService() == watcher) {
            return watch();
        }
        throw new IllegalArgumentException();
    }

    public AliyunOSSWatchKey watch() throws IOException {
        return fileSystem.provider().getWatchService().register(this);
    }

    public void watch(Function<AliyunOSSWatchEvent, Boolean> listener) throws IOException {
        fileSystem.provider().getWatchService().register(this, listener);
    }

    @Override
    public Iterator<Path> iterator() {
        return StreamSupport.stream(getNames().spliterator(), false)
                .map(self -> (Path) new AliyunOSSPath(fileSystem, self))
                .iterator();
    }

    @Override
    public int compareTo(Path other) {
        return toString().compareTo(other.toString());
    }

    @Override
    public String toString() {
        return isAbsolute() ? getVfsPath() : pathDescriptor;
    }

    @Override
    public boolean equals(Object object) {
        AliyunOSSPath that = (AliyunOSSPath) object;
        return that.fileSystem.equals(this.fileSystem) && that.pathDescriptor.equals(this.pathDescriptor);
    }

    @Override
    public int hashCode() {
        return fileSystem.hashCode() * 31 + pathDescriptor.hashCode();
    }

    public AliyunOSSFile toAliyunOSSFile() {
        AliyunOSSFileStore fileStore = getFileStore();
        return new AliyunOSSFile(fileStore.getAccessKeyId(), fileStore.getSecretAccessKey(),
                fileStore.getBucketName(), fileStore.getEndpoint(), getObjectKey());
    }
}
