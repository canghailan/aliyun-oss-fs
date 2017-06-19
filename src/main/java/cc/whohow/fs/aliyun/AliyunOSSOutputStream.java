package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.AppendObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

public class AliyunOSSOutputStream extends OutputStream {
    private final OSSClient client;
    private final String bucketName;
    private final String objectKey;
    private volatile long position;

    public AliyunOSSOutputStream(OSSClient client, String bucketName, String objectKey) {
        this.client = client;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.position = 0L;
    }

    public OSSClient getClient() {
        return client;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public long getPosition() {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b});
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        position = client.appendObject(
                new AppendObjectRequest(bucketName, objectKey, new ByteArrayInputStream(b, off, len))
                        .withPosition(position)).getNextPosition();
    }
}
