package cc.whohow.fs.aliyun;

import com.aliyun.oss.model.AppendObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

public class AliyunOSSOutputStream extends OutputStream {
    private final AliyunOSSPath path;
    private volatile long position;

    public AliyunOSSOutputStream(AliyunOSSPath path) {
        this.path = path;
        this.position = 0L;
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
        position = path.getClient().appendObject(
                new AppendObjectRequest(path.getBucketName(), path.getObjectKey(),
                        new ByteArrayInputStream(b, off, len))
                        .withPosition(position)).getNextPosition();
    }
}
