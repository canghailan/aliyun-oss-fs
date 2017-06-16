package cc.whohow.fs.aliyun;

import com.aliyun.oss.model.OSSObjectSummary;

import java.nio.file.Path;
import java.nio.file.WatchEvent;

/**
 * 阿里云文件监听事件
 */
public class AliyunOSSWatchEvent implements WatchEvent<Path> {
    private final long timestamp; // 事件时间戳
    private final Kind<Path> kind; // 事件类型
    private final OSSObjectSummary target; // 事件目标
    private final AliyunOSSPath watchable; // 事件监听者

    public AliyunOSSWatchEvent(AliyunOSSWatchEvent e, AliyunOSSPath watchable) {
        this.timestamp = e.timestamp;
        this.kind = e.kind;
        this.target = e.target;
        this.watchable = watchable;
    }

    public AliyunOSSWatchEvent(Kind<Path> kind, AliyunOSSPath watchable, OSSObjectSummary target) {
        this.timestamp = System.currentTimeMillis();
        this.kind = kind;
        this.target = target;
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
        return watchable.getFileSystem().getPath(getContext());
    }

    public String getContext() {
        if (watchable == null) {
            return null;
        }
        return target.getKey().substring(watchable.getObjectKey().length());
    }

    public long getTimestamp() {
        return timestamp;
    }

    public AliyunOSSPath getWatchable() {
        return watchable;
    }

    public AliyunOSSPath getTarget() {
        if (watchable == null) {
            return null;
        }
        return new AliyunOSSPath(watchable.getFileSystem(), "/" + target.getKey());
    }

    public String getBucket() {
        return target.getBucketName();
    }

    public String getObjectKey() {
        return target.getKey();
    }
}
