package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.*;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * OSS路径。以/开头为绝对路径，否则为相对路径。以/结尾为目录，否则为普通文件。
 */
public class AliyunOSSPath implements Path {
    private static final Pattern NAME = Pattern.compile("(?:^|/)([^/]*)"); // 路径名正则表达式
    private static final Pattern FILE_NAME = Pattern.compile("^(.*)([^/]*)/?$"); // 文件名正则表达式

    private final AliyunOSSFileSystem fileSystem; // 所属文件系统
    private final String pathDescriptor; // 路径描述符，绝对路径为 /ObjectKey，相对路径为name1/name2/.../nameN

    public AliyunOSSPath(AliyunOSSFileSystem fileSystem, String pathDescriptor) {
        this.fileSystem = fileSystem;
        this.pathDescriptor = pathDescriptor;
    }

    public OSSClient getClient() {
        return getFileStore().getOSSClient();
    }

    public String getBucketName() {
        return getFileStore().getBucketName();
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
        Matcher matcher = FILE_NAME.matcher(pathDescriptor);
        if (matcher.matches()) {
            return new AliyunOSSPath(fileSystem, matcher.group(2));
        }
        return null;
    }

    /**
     * 上级目录
     */
    @Override
    public AliyunOSSPath getParent() {
        Matcher matcher = FILE_NAME.matcher(pathDescriptor);
        if (matcher.matches()) {
            String parent = matcher.group(1);
            if (parent.isEmpty()) {
                return null;
            }
            return new AliyunOSSPath(fileSystem, parent);
        }
        return null;
    }

    @Override
    public int getNameCount() {
        return (int) StreamSupport.stream(Spliterators.spliteratorUnknownSize(nameIterator(), 0), false).count();
    }

    @Override
    public AliyunOSSPath getName(int index) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(nameIterator(), 0), false)
                .skip(index)
                .findFirst()
                .map(name -> new AliyunOSSPath(fileSystem, name))
                .orElse(null);
    }

    @Override
    public AliyunOSSPath subpath(int beginIndex, int endIndex) {
        String names = StreamSupport.stream(Spliterators.spliteratorUnknownSize(nameIterator(), 0), false)
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
            return new URL("http://" + getFileStore().getCname().get(0) + pathDescriptor);
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
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(nameIterator(), 0), false)
                .map(self -> (Path) new AliyunOSSPath(fileSystem, self))
                .iterator();
    }

    public Iterable<String> getNames() {
        return this::nameIterator;
    }

    public Iterator<String> nameIterator() {
        String names = toString().replaceAll("(^/|/$)", "");
        return new Iterator<String>() {
            Matcher matcher = NAME.matcher(names);

            @Override
            public boolean hasNext() {
                return matcher.find();
            }

            @Override
            public String next() {
                return matcher.group(1);
            }
        };
    }

    @Override
    public int compareTo(Path other) {
        return toString().compareTo(other.toString());
    }

    @Override
    public String toString() {
        return isAbsolute() ? getVfsPath() : pathDescriptor;
    }
}
