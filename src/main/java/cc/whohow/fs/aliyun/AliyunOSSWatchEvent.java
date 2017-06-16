package cc.whohow.fs.aliyun;

import java.nio.file.Path;
import java.nio.file.WatchEvent;

/**
 * 阿里云文件监听事件
 */
public class AliyunOSSWatchEvent implements WatchEvent<Path> {
    private final long timestamp; // 事件时间戳
    private final Kind<Path> kind; // 事件类型
    private final String targetUri; // 事件目标URI
    private final AliyunOSSFileStore fileStore; // 阿里云Bucket
    private final AliyunOSSPath watchable; // 事件监听者

    public AliyunOSSWatchEvent(AliyunOSSWatchEvent e, AliyunOSSPath watchable) {
        this.timestamp = e.timestamp;
        this.kind = e.kind;
        this.targetUri = e.targetUri;
        this.fileStore = e.fileStore;
        this.watchable = watchable;
    }

    public AliyunOSSWatchEvent(Kind<Path> kind, AliyunOSSFileStore fileStore, String targetUri, AliyunOSSPath watchable) {
        this.timestamp = System.currentTimeMillis();
        this.kind = kind;
        this.targetUri = targetUri;
        this.fileStore = fileStore;
        this.watchable = watchable;
    }

    @Override
    public Kind<Path> kind() {
        return kind;
    }

    @Override
    public int count() {
        return 1;
    }

    @Override
    public Path context() {
        if (watchable == null) {
            return null;
        }
        return watchable.getFileSystem().getPath(targetUri.substring(watchable.toUri().toString().length()));
    }

    public String getTargetUri() {
        return targetUri;
    }

    public AliyunOSSPath getWatchable() {
        return watchable;
    }

    public AliyunOSSPath getTarget() {
        return fileStore.provider().getPath(targetUri);
    }
}
