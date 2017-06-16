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
    private final AliyunOSSPath watchable; // 监听目录
    private volatile boolean valid; // 是否有效
    private volatile List<AliyunOSSWatchEvent> events = new CopyOnWriteArrayList<>(); // 事件列表

    public AliyunOSSWatchKey(AliyunOSSPath watchable) {
        this.watchable = watchable;
        this.valid = true;
    }

    @Override
    public boolean isValid() {
        return valid;
    }

    @Override
    public List<WatchEvent<?>> pollEvents() {
        // 保存当前事件列表
        List<WatchEvent<?>> r = Collections.unmodifiableList(events);
        // 启用新事件列表
        events = new CopyOnWriteArrayList<>();
        return r;
    }

    @Override
    public boolean reset() {
        if (valid) {
            events = new CopyOnWriteArrayList<>();
        }
        return valid;
    }

    @Override
    public void cancel() {
//        valid = false;
        throw new UnsupportedOperationException();
    }

    @Override
    public AliyunOSSPath watchable() {
        return watchable;
    }

    public boolean isSignalled() {
        return !events.isEmpty();
    }

    public void offerEvent(AliyunOSSWatchEvent e) {
        if (valid) {
            events.add(new AliyunOSSWatchEvent(e, watchable));
        }
    }
}
