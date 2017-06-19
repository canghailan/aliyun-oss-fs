package cc.whohow.fs.aliyun;

import cc.whohow.fs.FilterDirectoryStream;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.*;

import java.io.*;
import java.net.*;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * 阿里云OSS虚拟文件系统，支持多帐号、多Bucket、多文件夹
 */
public class AliyunOSSFileSystemProvider extends FileSystemProvider implements AutoCloseable {
    private final boolean intranet; // 是否内网环境
    private final Properties properties; // 配置文件
    // OSS客户端缓存，Key为accessKeyId:secretAccessKey@endpoint
    private final ConcurrentMap<String, OSSClient> clients = new ConcurrentHashMap<>();
    // Bucket缓存，Key为accessKeyId:secretAccessKey@bucketName.endpoint
    private final ConcurrentMap<String, AliyunOSSFileStore> fileStores = new ConcurrentHashMap<>();
    // 文件系统缓存及搜索表，Key为URI，按URI长度倒序及URI字典序排序
    private final ConcurrentMap<String, AliyunOSSFileSystem> fileSystems = new ConcurrentSkipListMap<>(
            Comparator.comparing(String::length).reversed().thenComparing(String::compareTo));
    // 线程池
    private volatile ScheduledExecutorService executor;
    // 文件监听服务
    private volatile AliyunOSSWatchService watchService;

    public AliyunOSSFileSystemProvider(Properties properties) {
        this.intranet = "intranet".equalsIgnoreCase(detectNetwork());
        this.properties = properties;

        // 拆分虚拟文件系统配置文件
        Map<String, Properties> keyProps = new HashMap<>();
        Properties defaults = new Properties(); // 公共配置
        for (String propertyName : properties.stringPropertyNames()) {
            String key; // 虚拟文件系统代号
            String name = propertyName; // 虚拟文件系统属性名
            Properties props = defaults;
            int sep = propertyName.indexOf('.');
            if (sep > 0) {
                key = propertyName.substring(0, sep);
                name = propertyName.substring(sep + 1);
                props = keyProps.computeIfAbsent(key, k -> new Properties(defaults));
            }
            props.put(name, properties.getProperty(propertyName));
        }
        // 初始化虚拟文件系统
        for (Properties props : keyProps.values()) {
            AliyunOSSFileSystem fileSystem = new AliyunOSSFileSystem(this, props);
            for (String uri : fileSystem.getAlternativeUris()) {
                fileSystems.put(uri, fileSystem);
            }
        }
    }

    /**
     * 探测网络环境
     *
     * @return intranet：内网，extranet：外网
     */
    public static String detectNetwork() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(
                    "oss-cn-hangzhou-internal.aliyuncs.com", 80), 1000);
            return "intranet";
        } catch (IOException ignore) {
            return "extranet";
        }
    }

    public boolean isIntranet() {
        return intranet;
    }

    /**
     * 获取OSS客户端，复用相同客户端
     */
    public OSSClient getOSSClient(String accessKeyId, String secretAccessKey, String endpoint) {
        String key = String.format("%s:%s@%s", accessKeyId, secretAccessKey, endpoint);
        return clients.computeIfAbsent(key, self -> new OSSClient(endpoint, accessKeyId, secretAccessKey));
    }

    /**
     * 获取FileStore，复用相同FileStore
     */
    public AliyunOSSFileStore getFileStore(Properties properties) {
        String accessKeyId = properties.getProperty("accessKeyId");
        String secretAccessKey = properties.getProperty("secretAccessKey");
        String endpoint = properties.getProperty("endpoint");
        String bucketName = properties.getProperty("bucketName");
        String key = String.format("%s:%s@%s.%s", accessKeyId, secretAccessKey, bucketName, endpoint);
        return fileStores.computeIfAbsent(key, self -> new AliyunOSSFileStore(this, properties));
    }

    public Collection<FileStore> getFileStores() {
        return Collections.unmodifiableCollection(fileStores.values());
    }

    public Collection<FileSystem> getFileSystems() {
        return Collections.unmodifiableCollection(new HashSet<>(fileSystems.values()));
    }

    public ScheduledExecutorService getExecutor() {
        if (executor == null) {
            synchronized (this) {
                if (executor == null) {
                    executor = Executors.newScheduledThreadPool(2);
                }
            }
        }
        return executor;
    }

    public AliyunOSSWatchService getWatchService() {
        if (watchService == null) {
            synchronized (this) {
                if (watchService == null) {
                    watchService = new AliyunOSSWatchService(this, properties);
                }
            }
        }
        return watchService;
    }

    @Override
    public AliyunOSSPath getPath(URI uri) {
        return getPath(uri.toString());
    }

    /**
     * 查找对应的路径
     */
    public AliyunOSSPath getPath(String uri) {
        return fileSystems.entrySet().stream() //
                // 前缀是否匹配
                .filter(self -> uri.startsWith(self.getKey()))
                // 去掉前缀，转为相对路径（虚拟路径）
                .map(self -> self.getValue().getPath("/" + uri.substring(self.getKey().length())))
                .findFirst()
                .orElse(null);
    }

    /**
     * 是否存在
     */
    public boolean exists(String uri) {
        return exists(getPath(uri));
    }

    /**
     * 是否存在
     */
    public boolean exists(AliyunOSSPath path) {
        return path.getClient().doesObjectExist(path.getBucketName(), path.getObjectKey());
    }

    /**
     * 拷贝（上传）
     */
    public void copy(File source, String target) {
        copy(source, getPath(target));
    }

    /**
     * 拷贝（上传）
     */
    public void copy(File source, AliyunOSSPath target) {
        target.getClient().putObject(target.getBucketName(), target.getObjectKey(), source);
    }

    /**
     * 拷贝（上传）
     */
    public void copy(InputStream source, String target) {
        copy(source, getPath(target));
    }

    /**
     * 拷贝（上传）
     */
    public void copy(InputStream source, AliyunOSSPath target) {
        target.getClient().putObject(target.getBucketName(), target.getObjectKey(), source);
    }

    /**
     * 拷贝（上传）文件夹，直接覆盖，小心使用
     */
    public void copyRecursively(File source, String target) throws IOException {
        copyRecursively(source, getPath(target));
    }

    /**
     * 拷贝（上传）文件夹，直接覆盖，小心使用
     */
    public void copyRecursively(File source, AliyunOSSPath target) throws IOException {
        if (!source.isDirectory() || target.isFile()) {
            throw new IllegalArgumentException();
        }
        Path src = source.toPath();
        OSSClient client = target.getClient();
        String prefix = target.getObjectKey();
        String bucketName = target.getBucketName();
        // 遍历文件夹
        Files.walk(src).parallel()
                // 过滤出普通文件
                .filter(Files::isRegularFile)
                .forEach(self -> {
                    // 相对路径
                    Path relative = src.relativize(self);
                    // 加上文件夹前缀，转为OSS中ObjectKey
                    String objectKey = prefix + relative.toString().replace(relative.getFileSystem().getSeparator(), "/");
                    // 上传
                    client.putObject(bucketName, objectKey, self.toFile());
                });
    }

    /**
     * 拷贝（下载）
     */
    public void copy(String source, File target) {
        copy(getPath(source), target);
    }

    /**
     * 拷贝（下载）
     */
    public void copy(AliyunOSSPath source, File target) {
        source.getClient().getObject(new GetObjectRequest(source.getBucketName(), source.getObjectKey()), target);
    }

    /**
     * 拷贝（下载）文件夹
     */
    public void copyRecursively(String source, File target) {
        copyRecursively(getPath(source), target);
    }

    /**
     * 拷贝（下载）文件夹
     */
    public void copyRecursively(AliyunOSSPath source, File target) {
        if (source.isFile() || !target.isDirectory()) {
            throw new IllegalArgumentException();
        }
        OSSClient client = source.getClient();
        int prefixLength = source.getObjectKey().length();
        // 遍历文件夹
        StreamSupport.stream(listObjectsRecursively(source).spliterator(), true)
                // 过滤文件夹
                .filter(self -> !self.getKey().endsWith("/"))
                .forEach(self -> {
                    File file = new File(target, self.getKey().substring(prefixLength));
                    File parent = file.getParentFile();
                    if (!parent.exists()) {
                        parent.mkdirs();
                    }
                    client.getObject(new GetObjectRequest(self.getBucketName(), self.getKey()), file);
                });
    }

    /**
     * 拷贝（上传）
     */
    public void copy(URL source, String target) throws IOException {
        copy(source, getPath(target));
    }

    /**
     * 拷贝（上传）
     */
    public void copy(URL source, AliyunOSSPath target) throws IOException {
        URLConnection connection = source.openConnection();
        ObjectMetadata objectMetadata = new ObjectMetadata();
        if (connection.getContentType() != null) {
            objectMetadata.setContentType(connection.getContentType());
        }
        if (connection.getContentLengthLong() >= 0) {
            objectMetadata.setContentLength(connection.getContentLengthLong());
        }
        try (InputStream stream = connection.getInputStream()) {
            target.getClient().putObject(target.getBucketName(), target.getObjectKey(), stream, objectMetadata);
        }
    }

    /**
     * 拷贝
     */
    public void copy(String source, String target) throws IOException {
        copy(getPath(source), getPath(target));
    }

    /**
     * 拷贝
     */
    public void copy(AliyunOSSPath source, AliyunOSSPath target) throws IOException {
        if (source.getClient() == target.getClient()) {
            // 同客户端拷贝
            source.getClient().copyObject(source.getBucketName(), source.getObjectKey(), target.getBucketName(), target.getObjectKey());
        } else {
            // 跨客户端拷贝，先下载，再上传
            OSSObject object = source.getClient().getObject(source.getBucketName(), source.getObjectKey());
            try (InputStream stream = object.getObjectContent()) {
                target.getClient().putObject(target.getBucketName(), target.getObjectKey(), stream, object.getObjectMetadata());
            }
        }
    }

    /**
     * 拷贝文件夹，直接覆盖，小心使用
     */
    public void copyRecursively(String source, String target)
            throws IOException {
        copyRecursively(getPath(source), getPath(target));
    }

    /**
     * 拷贝文件夹，直接覆盖，小心使用
     */
    public void copyRecursively(AliyunOSSPath source, AliyunOSSPath target) throws IOException {
        if (source.isFile() || target.isFile()) {
            throw new IllegalArgumentException();
        }
        int prefixLength = source.getObjectKey().length();
        // 遍历文件夹
        Stream<OSSObjectSummary> objects = StreamSupport.stream(listObjectsRecursively(source).spliterator(), true)
                .filter(self -> !self.getKey().endsWith("/"));
        if (source.getClient() == target.getClient()) {
            // 同客户端拷贝
            objects.forEach(self -> source.getClient().copyObject(
                    self.getBucketName(), self.getKey(),
                    target.getBucketName(), target.getObjectKey() + self.getKey().substring(prefixLength)));
        } else {
            // 跨客户端拷贝，先下载，再上传
            objects.forEach(self -> {
                OSSObject object = source.getClient().getObject(self.getBucketName(), self.getKey());
                try (InputStream stream = object.getObjectContent()) {
                    target.getClient().putObject(target.getBucketName(), target.getObjectKey(), stream, object.getObjectMetadata());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    /**
     * 删除
     */
    public void delete(String uri) {
        delete(getPath(uri));
    }

    /**
     * 删除
     */
    public void delete(AliyunOSSPath path) {
        path.getClient().deleteObject(path.getBucketName(), path.getObjectKey());
    }

    /**
     * 删除文件夹，小心使用
     */
    public void deleteRecursively(String uri) {
        deleteRecursively(getPath(uri));
    }

    /**
     * 删除文件夹，小心使用
     */
    public void deleteRecursively(AliyunOSSPath path) {
        OSSClient client = path.getClient();
        // 遍历文件夹
        StreamSupport.stream(listObjectsRecursively(path).spliterator(), true)
                .filter(self -> !self.getKey().endsWith("/"))
                .forEach(self -> client.deleteObject(self.getBucketName(), self.getKey()));
    }

    /**
     * 移动（重命名）
     */
    public void move(String source, String target) throws IOException {
        move(getPath(source), getPath(target));
    }

    /**
     * 移动（重命名）
     */
    public void move(AliyunOSSPath source, AliyunOSSPath target) throws IOException {
        // 拷贝、删除模拟移动
        copy(source, target);
        delete(source);
    }

    /**
     * 移动（重命名）文件夹，小心使用
     */
    public void moveRecursively(String source, String target)
            throws IOException {
        moveRecursively(getPath(source), getPath(target));
    }

    /**
     * 移动（重命名），小心使用
     */
    public void moveRecursively(AliyunOSSPath source, AliyunOSSPath target) throws IOException {
        // 先完成拷贝、再删除，保证数据完整性
        copyRecursively(source, target);
        deleteRecursively(source);
    }

    /**
     * 获取文件属性
     */
    public ObjectMetadata getMetadata(String uri) {
        return getMetadata(getPath(uri));
    }

    /**
     * 获取文件属性
     */
    public ObjectMetadata getMetadata(AliyunOSSPath path) {
        return path.getClient().getObjectMetadata(path.getBucketName(), path.getObjectKey());
    }

    /**
     * 设置文件属性
     */
    public void setMetadata(String uri, ObjectMetadata objectMetadata) {
       setMetadata(getPath(uri), objectMetadata);
    }

    /**
     * 设置文件属性
     */
    public void setMetadata(AliyunOSSPath path, ObjectMetadata objectMetadata) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(
                path.getBucketName(), path.getObjectKey(), path.getBucketName(), path.getObjectKey());
        copyObjectRequest.setNewObjectMetadata(objectMetadata);
        path.getClient().copyObject(copyObjectRequest);
    }

    /**
     * 获取文件或文件夹大小
     */
    public long getSize(String uri) {
        return getSize(getPath(uri));
    }

    /**
     * 获取文件或文件夹大小
     */
    public long getSize(AliyunOSSPath path) {
        if (path.isFile()) {
            return getMetadata(path).getContentLength();
        }
        return StreamSupport.stream(listObjectsRecursively(path).spliterator(), true)
                .filter(self -> !self.getKey().endsWith("/"))
                .mapToLong(OSSObjectSummary::getSize)
                .sum();
    }

    /**
     * 获取文件或文件夹文件数
     */
    public long getCount(String uri) {
        return getCount(getPath(uri));
    }

    /**
     * 获取文件或文件夹文件数
     */
    public long getCount(AliyunOSSPath path) {
        if (path.isFile()) {
            return 1L;
        }
        return StreamSupport.stream(listObjectsRecursively(path).spliterator(), true)
                .filter(self -> !self.getKey().endsWith("/"))
                .count();
    }

    /**
     * 获取文件或文件夹最后修改时间
     */
    public Date getLastModified(String uri) {
        return getLastModified(getPath(uri));
    }

    /**
     * 获取文件或文件夹最后修改时间
     */
    public Date getLastModified(AliyunOSSPath path) {
        if (path.isFile()) {
            return getMetadata(path).getLastModified();
        }
        return StreamSupport.stream(listObjectsRecursively(path).spliterator(), true)
                .map(OSSObjectSummary::getLastModified)
                .filter(Objects::nonNull)
                .max(Date::compareTo)
                .orElse(null);
    }

    /**
     * 读取文件
     */
    public InputStream newInputStream(String uri) {
        return newInputStream(getPath(uri));
    }

    /**
     * 读取文件
     */
    public InputStream newInputStream(AliyunOSSPath path) {
        return path.getClient().getObject(path.getBucketName(), path.getObjectKey()).getObjectContent();
    }

    /**
     * 读取文件内容
     */
    public byte[] getContent(String uri) throws IOException {
        return getContent(getPath(uri));
    }

    /**
     * 读取文件内容
     */
    public byte[] getContent(AliyunOSSPath path) throws IOException {
        try (InputStream stream = newInputStream(path)) {
            return IOUtils.readStreamAsByteArray(stream);
        }
    }

    /**
     * 读取文件内容
     */
    public String getContentAsString(String uri, String charset) throws IOException {
        return getContentAsString(getPath(uri), charset);
    }

    /**
     * 读取文件内容
     */
    public String getContentAsString(AliyunOSSPath path, String charset) throws IOException {
        try (InputStream stream = newInputStream(path)) {
            return IOUtils.readStreamAsString(stream, charset);
        }
    }

    /**
     * 写入文件
     */
    public OutputStream newOutputStream(String uri) {
        return newOutputStream(getPath(uri));
    }

    /**
     * 写入文件，文件需不存在，默认 128KB 缓冲区
     */
    public OutputStream newOutputStream(AliyunOSSPath path) {
        return new BufferedOutputStream(new AliyunOSSOutputStream(
                path.getClient(), path.getBucketName(), path.getObjectKey()), 128 * 1024);
    }

    /**
     * 写入文件
     */
    public void setContent(String uri, byte[] content) {
        setContent(getPath(uri), content);
    }

    /**
     * 写入文件
     */
    public void setContent(AliyunOSSPath path, byte[] content) {
        path.getClient().putObject(path.getBucketName(), path.getObjectKey(), new ByteArrayInputStream(content));
    }

    /**
     * 列出文件夹下所有文件OSS标准URI
     */
    public Iterable<String> list(String uri) {
        return list(getPath(uri));
    }

    /**
     * 列出文件夹下所有文件OSS标准URI
     */
    public Iterable<String> list(AliyunOSSPath path) {
        String base = path.getFileStore().getUri().toString();
        return () -> StreamSupport.stream(listObjects(path).spliterator(), false)
                .map(self -> base + "/" + self.getKey())
                .iterator();
    }

    /**
     * 列出文件夹下所有文件及目录路径
     */
    public Iterable<AliyunOSSPath> listPath(String uri) {
        return listPath(getPath(uri));
    }

    /**
     * 列出文件夹下所有文件及目录路径
     */
    public Iterable<AliyunOSSPath> listPath(AliyunOSSPath path) {
        return () -> StreamSupport.stream(listObjects(path).spliterator(), false)
                .map(self -> new AliyunOSSPath(path.getFileSystem(), "/" + self.getKey()))
                .iterator();
    }

    /**
     * 列出文件夹下所有文件及目录
     */
    public Iterable<OSSObjectSummary> listObjects(String uri) {
        return listObjects(getPath(uri));
    }

    /**
     * 列出文件夹下所有文件及目录
     */
    public Iterable<OSSObjectSummary> listObjects(AliyunOSSPath path) {
        return () -> new AliyunOSSObjectSummaryIterator(path.getClient(), path.getBucketName(), path.getObjectKey(), "/");
    }

    /**
     * 列出文件夹下所有文件OSS标准URI
     */
    public Iterable<String> listRecursively(String uri) {
        return listRecursively(getPath(uri));
    }

    /**
     * 列出文件夹下所有文件OSS标准URI
     */
    public Iterable<String> listRecursively(AliyunOSSPath path) {
        String base = path.getFileStore().getUri().toString();
        return () -> StreamSupport.stream(listObjectsRecursively(path).spliterator(), false)
                .map(self -> base + "/" + self.getKey())
                .iterator();
    }

    /**
     * 列出文件夹下所有文件及目录路径
     */
    public Iterable<AliyunOSSPath> listPathRecursively(String uri) {
        return listPathRecursively(getPath(uri));
    }

    /**
     * 列出文件夹下所有文件及目录路径
     */
    public Iterable<AliyunOSSPath> listPathRecursively(AliyunOSSPath path) {
        return () -> StreamSupport.stream(listObjectsRecursively(path).spliterator(), false)
                .map(self -> new AliyunOSSPath(path.getFileSystem(), "/" + self.getKey()))
                .iterator();
    }

    /**
     * 递归列出文件夹下所有文件及目录
     */
    public Iterable<OSSObjectSummary> listObjectsRecursively(String uri) {
        return listObjectsRecursively(getPath(uri));
    }

    /**
     * 递归列出文件夹下所有文件及目录
     */
    public Iterable<OSSObjectSummary> listObjectsRecursively(AliyunOSSPath path) {
        return () -> new AliyunOSSObjectSummaryIterator(path.getClient(), path.getBucketName(), path.getObjectKey());
    }

    @Override
    public String getScheme() {
        return "http";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AliyunOSSFileSystem getFileSystem(URI uri) {
        AliyunOSSPath path = getPath(uri);
        return path == null ? null : path.getFileSystem();
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>[] attrs) throws IOException {
        AliyunOSSPath aliyunOSSPath = (AliyunOSSPath) path;
        return new AliyunOSSFileChannel(aliyunOSSPath.getClient(), aliyunOSSPath.getBucketName(), aliyunOSSPath.getObjectKey());
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        AliyunOSSDirectoryStream directoryStream = new AliyunOSSDirectoryStream((AliyunOSSPath) dir);
        return filter == null ? directoryStream : new FilterDirectoryStream<>(directoryStream, filter);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>[] attrs) throws IOException {
        AliyunOSSPath aliyunOSSPath = (AliyunOSSPath) dir;
        if (aliyunOSSPath.isFile()) {
            throw new IllegalArgumentException();
        }
        // do nothing
    }

    @Override
    public void delete(Path path) throws IOException {
        AliyunOSSPath aliyunOSSPath = (AliyunOSSPath) path;
        if (aliyunOSSPath.isFile()) {
            delete(aliyunOSSPath);
        } else {
            deleteRecursively(aliyunOSSPath);
        }
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        if (source instanceof AliyunOSSPath) {
            AliyunOSSPath sourcePath = (AliyunOSSPath) source;
            if (target instanceof AliyunOSSPath) {
                AliyunOSSPath targetPath = (AliyunOSSPath) target;
                if (sourcePath.isFile()) {
                    copy(sourcePath, targetPath);
                } else {
                    copyRecursively(sourcePath, targetPath);
                }
            } else {
                if (sourcePath.isFile()) {
                    copy(sourcePath, target.toFile());
                } else {
                    copyRecursively(sourcePath, target.toFile());
                }
            }
        } else {
            if (target instanceof AliyunOSSPath) {
                AliyunOSSPath targetPath = (AliyunOSSPath) target;
                if (targetPath.isFile()) {
                    copy(source.toFile(), targetPath);
                } else {
                    copyRecursively(source.toFile(), targetPath);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        if (source instanceof AliyunOSSPath) {
            AliyunOSSPath sourcePath = (AliyunOSSPath) source;
            if (target instanceof AliyunOSSPath) {
                AliyunOSSPath targetPath = (AliyunOSSPath) target;
                if (sourcePath.isFile()) {
                    move(sourcePath, targetPath);
                } else {
                    moveRecursively(sourcePath, targetPath);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return path instanceof AliyunOSSPath
                && path2 instanceof AliyunOSSPath
                && path.toUri().equals(path2.toUri());
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        AliyunOSSPath aliyunOSSPath = (AliyunOSSPath) path;
        return aliyunOSSPath.getFileStore();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        // do nothing
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return (V) new AliyunOSSFileAttributes((AliyunOSSPath) path);
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        return (A) new AliyunOSSFileAttributes((AliyunOSSPath) path);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        return getMetadata((AliyunOSSPath) path).getRawMetadata();
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        closeExecutor();
        closeWatchService();
        closeFileSystems();
        closeClients();
    }

    private void closeExecutor() {
        if (executor != null) {
            try {
                executor.shutdownNow();
                executor.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
            }
        }
    }

    private void closeWatchService() {
        if (watchService != null) {
            try {
                watchService.close();
            } catch (Throwable ignore){
            }
        }
    }

    private void closeFileSystems() {
        for (FileSystem fileSystem : getFileSystems()) {
            try {
                fileSystem.close();
            }  catch (Throwable ignore){
            }
        }
    }

    private void closeClients() {
        for (OSSClient client : clients.values()) {
            try {
                client.shutdown();
            } catch (Throwable ignore) {
            }
        }
    }
}
