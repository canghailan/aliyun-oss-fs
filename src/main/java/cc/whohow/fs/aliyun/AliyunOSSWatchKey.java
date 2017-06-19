package cc.whohow.fs.aliyun;

import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 阿里云文件监听点
 */
public class AliyunOSSWatchKey implements WatchKey {
    private final AliyunOSSWatchService watchService; // 监听服务
    private final AliyunOSSPath watchable; // 监听目录
    private volatile boolean valid; // 是否有效
    private volatile List<AliyunOSSWatchEvent> pendingEvents = new CopyOnWriteArrayList<>(); // 事件接收队列

    public AliyunOSSWatchKey(AliyunOSSWatchService watchService, AliyunOSSPath watchable) {
        this.watchService = watchService;
        this.watchable = watchable;
        this.valid = true;
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public List<WatchEvent<?>> pollEvents() {
        List<WatchEvent<?>> events = Collections.unmodifiableList(pendingEvents); // 保存当前事件列表
        pendingEvents = new CopyOnWriteArrayList<>(); // 启用新事件列表
        return events;
    }

    @Override
    public boolean reset() {
        if (isValid()) {
            pendingEvents = new CopyOnWriteArrayList<>();
            return true;
        }
        return false;
    }

    @Override
    public void cancel() {
        watchService.cancel(this);
        valid = false;
    }

    @Override
    public AliyunOSSPath watchable() {
        return watchable;
    }

    public boolean isSignalled() {
        return !pendingEvents.isEmpty();
    }

    public void offerEvent(AliyunOSSWatchEvent e) {
        pendingEvents.add(new AliyunOSSWatchEvent(e, watchable));
    }
}
