package cc.whohow.fs.aliyun;

import java.io.IOException;
import java.nio.file.NotDirectoryException;
import java.nio.file.WatchService;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * 阿里云文件监听服务
 */
public class AliyunOSSWatchService implements WatchService {
    private final AliyunOSSFileSystemProvider fileSystemProvider;
    private final Properties properties;
    private final ScheduledExecutorService executor;

    private final List<AliyunOSSWatchTask> tasks = new CopyOnWriteArrayList<>(); // 监听任务集合
    private final Map<String, List<AliyunOSSWatchKey>> watchKeys = new ConcurrentHashMap<>(); // 监听点集合
    private final Map<String, List<Function<AliyunOSSWatchEvent, Boolean>>> listeners = new ConcurrentHashMap<>(); // 监听回调集合
    private final BlockingQueue<AliyunOSSWatchKey> watchKeyQueue = new LinkedBlockingQueue<>(); // 监听点队列

    public AliyunOSSWatchService(AliyunOSSFileSystemProvider fileSystemProvider, Properties properties) {
        this.fileSystemProvider = fileSystemProvider;
        this.properties = properties;
        this.executor = fileSystemProvider.getExecutor();
    }

    public AliyunOSSFileSystemProvider provider() {
        return fileSystemProvider;
    }

    /**
     * 监听轮询间隔
     */
    public long getWatchInterval() {
        return Long.parseLong(properties.getProperty("watch-interval", "60000"));
    }

    @Override
    public synchronized void close() throws IOException {
            for (AliyunOSSWatchTask task : tasks) {
                try {
                    task.stop();
                } catch (Throwable ignore) {
                }
            }
    }

    @Override
    public AliyunOSSWatchKey poll() {
        while (true) {
            AliyunOSSWatchKey watchKey = watchKeyQueue.poll();
            if (watchKey == null) {
                return null;
            }
            if (watchKey.isSignalled()) {
                watchKeyQueue.removeIf(e -> e == watchKey); // 移除重复
                return watchKey;
            }
        }
    }

    @Override
    public AliyunOSSWatchKey poll(long timeout, TimeUnit unit) throws InterruptedException {
        long timestamp = System.currentTimeMillis() + unit.toMillis(timeout);
        while (true) {
            long t = timestamp - System.currentTimeMillis();
            if (t < 0) {
                return null;
            }
            AliyunOSSWatchKey watchKey = watchKeyQueue.poll(t, unit);
            if (watchKey == null) {
                return null;
            }
            if (watchKey.isSignalled()) {
                watchKeyQueue.removeIf(e -> e == watchKey);
                return watchKey;
            }
        }
    }

    @Override
    public AliyunOSSWatchKey take() throws InterruptedException {
        while (true) {
            AliyunOSSWatchKey watchKey = watchKeyQueue.take();
            if (watchKey.isSignalled()) {
                watchKeyQueue.removeIf(e -> e == watchKey);
                return watchKey;
            }
        }
    }

    /**
     * 注册监听点
     */
    public synchronized AliyunOSSWatchKey register(AliyunOSSPath path) throws IOException {
        if (path.isFile()) {
            throw new NotDirectoryException(path.toString());
        }
        AliyunOSSWatchKey watchKey = new AliyunOSSWatchKey(this, path);
        watchKeys.computeIfAbsent(path.toUri().toString(), s -> new CopyOnWriteArrayList<>()).add(watchKey);
        watch(path);
        return watchKey;
    }

    /**
     * 注册监听回调
     */
    public synchronized void register(AliyunOSSPath path, Function<AliyunOSSWatchEvent, Boolean> listener) {
        listeners.computeIfAbsent(path.toUri().toString(), s -> new CopyOnWriteArrayList<>()).add(listener);
        watch(path);
    }

    /**
     * 注销监听点
     */
    public synchronized void cancel(AliyunOSSWatchKey watchKey) {
        String uri = watchKey.watchable().toUri().toString();
        List<AliyunOSSWatchKey> uriWatchKeys = watchKeys.get(uri);
        if (uriWatchKeys == null || uriWatchKeys.isEmpty()) {
            throw new IllegalStateException();
        }
        if (!uriWatchKeys.remove(watchKey)) {
            throw new IllegalStateException();
        }
        unwatch(uri);
    }

    /**
     * 提交监听任务
     */
    private synchronized void watch(AliyunOSSPath path) {
        String uri = path.toUri().toString();
        for (AliyunOSSWatchTask task : tasks) {
            if (task.accept(uri)) {
                task.addWatchObjectKey(path.getObjectKey());
                return;
            }
        }
        AliyunOSSPath watchable = path.isFile() ? path.getParent() : path;
        AliyunOSSWatchTask task = new AliyunOSSWatchTask(this, watchable.getClient(),
                watchable.getBucketName(), watchable.getFileStore().getExtranetEndpoint(), watchable.getObjectKey());
        if (path != watchable) {
            task.addWatchObjectKey(path.getObjectKey());
        }
        task.start(executor, getWatchInterval(), TimeUnit.MILLISECONDS);
        tasks.add(task);
    }

    /**
     * 取消监听任务
     */
    private synchronized void unwatch(String uri) {
        List<AliyunOSSWatchKey> uriWatchKeys = watchKeys.get(uri);
        if (uriWatchKeys != null && uriWatchKeys.isEmpty()) {
            watchKeys.remove(uri, uriWatchKeys);
            uriWatchKeys = null;
        }
        List<Function<AliyunOSSWatchEvent, Boolean>> uriListeners = listeners.get(uri);
        if (uriListeners != null && uriListeners.isEmpty()) {
            listeners.remove(uri, uriListeners);
            uriListeners = null;
        }
        if (uriWatchKeys == null && uriListeners == null) {
            Iterator<AliyunOSSWatchTask> iterator = tasks.iterator();
            while (iterator.hasNext()){
                AliyunOSSWatchTask task = iterator.next();
                if (task.accept(uri)) {
                    String objectKey = uri.substring(task.getWatchBucketUri().length());
                    task.removeWatchObjectKey(objectKey);
                    if (task.getWatchObjectKeys().isEmpty()) {
                        try {
                            task.stop();
                        } finally {
                            iterator.remove();
                        }
                    }
                }
            }
        }
    }

    /**
     * 监听事件分发
     */
    void dispatchEvents(String target, AliyunOSSWatchEvent event) {
        List<AliyunOSSWatchKey> targetWatchKeys = watchKeys.get(target);
        if (targetWatchKeys != null && !targetWatchKeys.isEmpty()) {
            for (AliyunOSSWatchKey watchKey : targetWatchKeys) {
                try {
                    watchKey.offerEvent(event);
                    watchKeyQueue.offer(watchKey);
                } catch (Throwable ignore) {
                }
            }
        }
        List<Function<AliyunOSSWatchEvent, Boolean>> targetListeners = listeners.get(target);
        if (targetListeners != null && !targetListeners.isEmpty()) {
            for (Function<AliyunOSSWatchEvent, Boolean> listener : targetListeners) {
                try {
                    executor.submit(() -> {
                        if (Boolean.FALSE.equals(listener.apply(event))) {
                            targetListeners.remove(listener);
                            unwatch(target);
                        }
                    });
                } catch (Throwable ignore) {
                }
            }
        }
    }
}
