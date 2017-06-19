package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObjectSummary;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 阿里云文件监听任务
 */
public class AliyunOSSWatchTask implements Runnable {
    private final AliyunOSSWatchService watchService;
    private final OSSClient client;
    private final String watchBucketUri;
    private final String watchBucketName;
    private volatile ScheduledFuture<?> future;

    private final NavigableSet<String> watchObjectKeys = new ConcurrentSkipListSet<>(); // 需监听的ObjectKey集合
    private volatile String watchObjectKey; // 监听根目录
    private volatile Map<String, OSSObjectSummary> watchObjects; // 监听对象集合

    public AliyunOSSWatchTask(AliyunOSSWatchService watchService, OSSClient client,
                              String watchBucketName, String watchEndpoint, String watchObjectKey) {
        if (!watchObjectKey.endsWith("/")) {
            throw new IllegalArgumentException();
        }
        this.watchService = watchService;
        this.client = client;
        this.watchBucketUri = String.format("http://%s.%s/", watchBucketName, watchEndpoint);
        this.watchBucketName = watchBucketName;
        this.watchObjectKeys.add(watchObjectKey);
        this.watchObjectKey = watchObjectKey;
    }

    /**
     * 是否可重用此任务
     */
    public boolean accept(String uri) {
        String watchUri = getWatchUri();
        return watchUri.startsWith(uri) || uri.startsWith(watchUri);
    }

    /**
     * 当前监听根对象
     */
    public String getWatchUri() {
        return watchBucketUri + watchObjectKeys.first();
    }

    public String getWatchBucketUri() {
        return watchBucketUri;
    }

    /**
     * 新增监听对象
     */
    public void addWatchObjectKey(String objectKey) {
        String rootObjectKey = watchObjectKeys.first();
        if (rootObjectKey.startsWith(objectKey) || objectKey.startsWith(rootObjectKey)) {
            watchObjectKeys.add(objectKey);
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * 移除监听对象
     */
    public void removeWatchObjectKey(String objectKey) {
        watchObjectKeys.remove(objectKey);
    }

    /**
     * 获取所有监听对象
     */
    public Collection<String> getWatchObjectKeys() {
        return watchObjectKeys;
    }

    /**
     * 开始
     */
    public void start(ScheduledExecutorService executor, long interval, TimeUnit unit) {
        future = executor.scheduleWithFixedDelay(this, 0, interval, unit);
    }

    /**
     * 停止
     */
    public void stop() {
        future.cancel(true);
    }

    @Override
    public void run() {
        // 保存上次状态
        String prevWatchObjectKey = watchObjectKey;
        Map<String, OSSObjectSummary> prevWatchObjects = watchObjects;

        // 读取当前状态
        String currWatchObjectKey = watchObjectKeys.first();
        Map<String, OSSObjectSummary> currWatchObjects = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new AliyunOSSObjectSummaryIterator(client, watchBucketName, currWatchObjectKey), 0), false)
                .collect(Collectors.toMap(OSSObjectSummary::getKey, self -> self));

        // 更新状态
        watchObjectKey = currWatchObjectKey;
        watchObjects  = currWatchObjects;

        if (prevWatchObjects == null) {
            return;
        }

        boolean watchObjectKeyNotChange = currWatchObjectKey.equals(prevWatchObjectKey);
        for (OSSObjectSummary c : currWatchObjects.values()) {
            OSSObjectSummary p = prevWatchObjects.remove(c.getKey());
            if (p == null) {
                // 上次记录中文件不存在，文件新增或监听范围扩大
                if (watchObjectKeyNotChange || c.getKey().startsWith(prevWatchObjectKey)) {
                    dispatchEvents(prevWatchObjectKey, StandardWatchEventKinds.ENTRY_CREATE, c);
                }
            } else if (!Objects.equals(p.getETag(), c.getETag())) {
                // ETag变化，文件被修改
                dispatchEvents(prevWatchObjectKey, StandardWatchEventKinds.ENTRY_MODIFY, c);
            }
        }
        for (OSSObjectSummary p : prevWatchObjects.values()) {
            // 本次文件不存在，文件被删除或监听范围缩小
            if (watchObjectKeyNotChange || p.getKey().startsWith(currWatchObjectKey)) {
                dispatchEvents(prevWatchObjectKey, StandardWatchEventKinds.ENTRY_DELETE, p);
            }
        }
    }

    /**
     * 事件分发
     */
    private void dispatchEvents(String root, WatchEvent.Kind<Path> kind, OSSObjectSummary object) {
        AliyunOSSWatchEvent event = new AliyunOSSWatchEvent(kind, watchService.provider(), watchBucketUri + object.getKey(), null);
        for (String objectKey : watchObjectKeys.tailSet(root)) {
            // 只通知监听范围内的事件
            if (objectKey.startsWith(root)) {
                watchService.dispatchEvents(watchBucketUri + objectKey, event);
            } else {
                break;
            }
        }
    }
}
