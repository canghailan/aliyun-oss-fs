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
    private final ScheduledExecutorService executor; // 线程池
    private final Map<AliyunOSSWatchTask, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>(); // 监听任务集合
    private final Map<String, List<AliyunOSSWatchKey>> keys = new ConcurrentHashMap<>(); // 监听点集合
    private final BlockingQueue<AliyunOSSWatchKey> keyQueue = new LinkedBlockingQueue<>(); // 监听队列
    private final Map<String, List<Function<AliyunOSSWatchEvent, ?>>> listeners = new ConcurrentHashMap<>(); // 监听回调集合

    private final long interval; // 监听时间间隔

    public AliyunOSSWatchService(long interval) {
        this(Executors.newScheduledThreadPool(2), interval);
    }

    public AliyunOSSWatchService(ScheduledExecutorService executor, long interval) {
        this.executor = executor;
        this.interval = interval;
    }

    @Override
    public void close() throws IOException {
        try {
            executor.shutdownNow();
            executor.awaitTermination(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {
        }
    }

    @Override
    public AliyunOSSWatchKey poll() {
        while (true) {
            AliyunOSSWatchKey key = keyQueue.poll();
            if (key == null) {
                return null;
            }
            if (key.isSignalled()) {
                keyQueue.removeIf(e -> e == key); // 移除重复
                return key;
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
            AliyunOSSWatchKey key = keyQueue.poll(t, unit);
            if (key == null) {
                return null;
            }
            if (key.isSignalled()) {
                keyQueue.removeIf(e -> e == key);
                return key;
            }
        }
    }

    @Override
    public AliyunOSSWatchKey take() throws InterruptedException {
        while (true) {
            AliyunOSSWatchKey key = keyQueue.take();
            if (key.isSignalled()) {
                keyQueue.removeIf(e -> e == key);
                return key;
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
        List<AliyunOSSWatchKey> list = keys.computeIfAbsent(path.toUri().toString(), s -> new CopyOnWriteArrayList<>());
        for (AliyunOSSWatchKey k : list) {
            if (k.watchable().equals(path)) {
                // 返回已存在的监听点
                return k;
            }
        }
        AliyunOSSWatchKey k = new AliyunOSSWatchKey(path);
        list.add(k);
        watch(path);
        return k;
    }

    /**
     * 注册监听回调
     */
    public synchronized void register(AliyunOSSPath path, Function<AliyunOSSWatchEvent, ?> listener) {
        listeners.computeIfAbsent(path.toUri().toString(), s -> new CopyOnWriteArrayList<>()).add(listener);
        watch(path);
    }

    /**
     * 提交监听任务
     */
    private synchronized void watch(AliyunOSSPath path) {
        for (AliyunOSSWatchTask task : tasks.keySet()) {
            if (task.registerIfAccept(path)) {
                return;
            }
        }
        AliyunOSSWatchTask task = new AliyunOSSWatchTask(this, path.getFileStore());
        if (path.isFile()) {
            task.registerIfAccept(path.getParent());
        }
        task.registerIfAccept(path);
        ScheduledFuture<?> future = executor.scheduleWithFixedDelay(task, 0, interval, TimeUnit.MILLISECONDS);
        tasks.put(task, future);
    }

    /**
     * 监听事件分发
     */
    void dispatchEvents(String target, AliyunOSSWatchEvent event) {
        for (AliyunOSSWatchKey key : keys.getOrDefault(target, Collections.emptyList())) {
            try {
                key.offerEvent(event);
                keyQueue.offer(key);
            } catch (Throwable ignore) {
            }
        }
        for (Function<AliyunOSSWatchEvent, ?> listener : listeners.getOrDefault(target, Collections.emptyList())) {
            try {
                executor.submit(() -> listener.apply(event));
            } catch (Throwable ignore) {
            }
        }
    }
}
