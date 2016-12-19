package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * OSS对象遍历器
 */
public class AliyunOSSObjectSummaryIterator implements Iterator<OSSObjectSummary> {
    private final OSSClient client; // OSS客户端
    private volatile ListObjectsRequest listObjectsRequest; // 遍历请求
    private volatile ObjectListing objectListing; // 遍历结果
    private volatile Iterator<OSSObjectSummary> current; // 当前结果

    public AliyunOSSObjectSummaryIterator(OSSClient client, String bucketName, String prefix) {
        this(client, bucketName, prefix, null);
    }

    public AliyunOSSObjectSummaryIterator(OSSClient client, String bucketName, String prefix, String delimiter) {
        this(client, new ListObjectsRequest(bucketName, prefix, null, delimiter, 1000));
    }

    public AliyunOSSObjectSummaryIterator(OSSClient client, ListObjectsRequest listObjectsRequest) {
        this.client = client;
        this.listObjectsRequest = listObjectsRequest;
        this.objectListing = client.listObjects(listObjectsRequest);
        this.current = currentIterator();
    }

    private Iterator<OSSObjectSummary> currentIterator() {
        // 没有目录
        if (objectListing.getCommonPrefixes().isEmpty()) {
            return objectListing.getObjectSummaries().iterator();
        }
        // 有目录
        return Stream.concat(
                objectListing.getCommonPrefixes().stream().map(self -> {
                    OSSObjectSummary objectSummary = new OSSObjectSummary();
                    objectSummary.setBucketName(objectListing.getBucketName());
                    objectSummary.setKey(self);
                    return objectSummary;
                }),
                objectListing.getObjectSummaries().stream())
                .iterator();
    }

    @Override
    public boolean hasNext() {
        // 首先检查当前请求结果
        if (current.hasNext()) {
            return true;
        }
        // 是否有更多解结果，需要再次请求OSS
        if (objectListing.getNextMarker() == null) {
            return false;
        }
        // 请求OSS，获取下一批数据
        listObjectsRequest.setMarker(objectListing.getNextMarker());
        objectListing = client.listObjects(listObjectsRequest);
        current = currentIterator();
        return current.hasNext();
    }

    @Override
    public OSSObjectSummary next() {
        return current.next();
    }
}