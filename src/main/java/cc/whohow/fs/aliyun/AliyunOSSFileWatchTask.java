package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.SimplifiedObjectMeta;

import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

public class AliyunOSSFileWatchTask implements Runnable {
    private final AliyunOSSFile file;
    private final Function<AliyunOSSFileWatchTask, Boolean> stop;
    private final BiFunction<WatchEvent.Kind<?>, AliyunOSSFile, ?> listener;
    private final long startTime;
    private final AtomicInteger runningCount = new AtomicInteger(0);
    private volatile SimplifiedObjectMeta objectMeta;

    public AliyunOSSFileWatchTask(AliyunOSSFile file, Function<AliyunOSSFileWatchTask, Boolean> stop, BiFunction<WatchEvent.Kind<?>, AliyunOSSFile, ?> listener) {
        this.file = file;
        this.stop = stop;
        this.listener = listener;
        this.startTime = System.currentTimeMillis();
        this.objectMeta = getObjectMeta();
    }

    public long getRunningTime() {
        return System.currentTimeMillis() - startTime;
    }

    public int getRunningCount() {
        return runningCount.get();
    }

    private SimplifiedObjectMeta getObjectMeta() {
        try {
            return file.getObjectMeta();
        } catch (OSSException e) {
            if (OSSErrorCode.NO_SUCH_BUCKET.equals(e.getErrorCode())
                    ||OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public void run() {
        if (stop.apply(this)) {
            file.unwatch();
            return;
        }
        runningCount.getAndIncrement();

        SimplifiedObjectMeta prevObjectMeta = objectMeta;
        objectMeta = getObjectMeta();
        if (prevObjectMeta == null && objectMeta != null) {
            listener.apply(StandardWatchEventKinds.ENTRY_CREATE, file);
        } else if (prevObjectMeta != null && objectMeta == null) {
            listener.apply(StandardWatchEventKinds.ENTRY_DELETE, file);
        } else if (prevObjectMeta != null && objectMeta != null) {
            if (!Objects.equals(prevObjectMeta.getETag(), objectMeta.getETag())) {
                listener.apply(StandardWatchEventKinds.ENTRY_MODIFY, file);
            }
        }
    }

    public static class MaxTime implements Function<AliyunOSSFileWatchTask, Boolean> {
        private final long maxTime;

        public MaxTime(long maxTime) {
            this.maxTime = maxTime;
        }

        @Override
        public Boolean apply(AliyunOSSFileWatchTask task) {
            return task.getRunningTime() >= maxTime;
        }
    }

    public static class MaxCount implements Function<AliyunOSSFileWatchTask, Boolean> {
        private final int maxCount;

        public MaxCount(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public Boolean apply(AliyunOSSFileWatchTask task) {
            return task.getRunningCount() >= maxCount;
        }
    }
}
