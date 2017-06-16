package cc.whohow.fs.aliyun;

import com.aliyun.oss.model.OSSObjectSummary;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * 阿里云文件监听任务
 */
public class AliyunOSSWatchTask implements Runnable {
    private final AliyunOSSWatchService watchService;
    private final AliyunOSSFileStore fileStore; // 监听的Bucket
    private final NavigableMap<String, String> watchUris = new ConcurrentSkipListMap<>(); // 注册的需监听的URI，<ObjectKey, URI>
    private volatile String watchRoot; // 监听根目录
    private volatile Map<String, OSSObjectSummary> watchObjects; // 监听对象集合

    public AliyunOSSWatchTask(AliyunOSSWatchService watchService, AliyunOSSFileStore fileStore) {
        this.watchService = watchService;
        this.fileStore = fileStore;
        this.watchObjects = null;
    }

    public boolean registerIfAccept(AliyunOSSPath path) {
        // 只接受同Bucket的监听任务
        if (path.getBucketName().equals(fileStore.getBucketName())) {
            if (watchUris.isEmpty()) {
                watchUris.put(path.getObjectKey(), path.toUri().toString());
                return true;
            }
            // 只接受上级目录及下级目录，如果是上级目录，自动切换为根目录
            String firstKey = watchUris.firstKey();
            String objectKey = path.getObjectKey();
            if (firstKey.startsWith(objectKey) || objectKey.startsWith(firstKey)) {
                watchUris.put(path.getObjectKey(), path.toUri().toString());
                return true;
            }
        }
        return false;
    }

    public Collection<String> getWatchUris() {
        return watchUris.values();
    }

    @Override
    public void run() {
        // 保存上次监听情况
        String prevRoot = watchRoot;
        Map<String, OSSObjectSummary> prevObjects = watchObjects;

        // 读取当前文件状态
        watchRoot = watchUris.firstKey();
        watchObjects = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new AliyunOSSObjectSummaryIterator(fileStore.getOSSClient(), fileStore.getBucketName(), watchRoot), 0), false)
                .collect(Collectors.toMap(OSSObjectSummary::getKey, self -> self));

        if (prevObjects == null) {
            return;
        }

        for (OSSObjectSummary c : watchObjects.values()) {
            OSSObjectSummary p = prevObjects.remove(c.getKey());
            if (p == null) {
                // 上次记录中文件不存在，文件新增或监听范围扩大
                dispatchEvents(prevRoot, StandardWatchEventKinds.ENTRY_CREATE, c);
            } else if (!Objects.equals(p.getETag(), c.getETag())) {
                // ETag变化，文件被修改
                dispatchEvents(prevRoot, StandardWatchEventKinds.ENTRY_MODIFY, c);
            }
        }
        for (OSSObjectSummary p : prevObjects.values()) {
            // 本次文件不存在，文件被删除
            dispatchEvents(prevRoot, StandardWatchEventKinds.ENTRY_DELETE, p);
        }
    }

    /**
     * 事件分发
     */
    private void dispatchEvents(String root, WatchEvent.Kind<Path> kind, OSSObjectSummary object) {
        String targetUri = fileStore.getUri().toString() + "/" + object.getKey();
        AliyunOSSWatchEvent event = new AliyunOSSWatchEvent(kind, fileStore, targetUri, null);
        for (Map.Entry<String, String> uri : watchUris.tailMap(root).entrySet()) {
            // 只通知监听范围内的事件
            if (uri.getKey().startsWith(root)) {
                watchService.dispatchEvents(uri.getValue(), event);
            } else {
                break;
            }
        }
    }
}
