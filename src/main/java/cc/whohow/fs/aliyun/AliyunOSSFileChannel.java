package cc.whohow.fs.aliyun;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;

public class AliyunOSSFileChannel implements SeekableByteChannel {
    private final OSSClient client;
    private final String bucketName;
    private final String objectKey;
    private final File tempFile; // 临时文件
    private final FileChannel tempFileChannel; // 临时文件Channel
    private volatile boolean modified; // 文件是否被修改

    public AliyunOSSFileChannel(OSSClient client, String bucketName, String objectKey) throws IOException {
        if (objectKey.endsWith("/")) {
            throw new IOException("NotFile");
        }
        this.client = client;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.tempFile = File.createTempFile("AliyunOSSFileChannel", null);
        try {
            client.getObject(new GetObjectRequest(bucketName, objectKey), this.tempFile);
        } catch (OSSException e) {
            if (!OSSErrorCode.NO_SUCH_BUCKET.equals(e.getErrorCode())
                    && !OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
                throw e;
            }
        }
        this.tempFileChannel = FileChannel.open(tempFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.modified = false;
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

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return tempFileChannel.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        modified = true;
        return tempFileChannel.write(src);
    }

    @Override
    public long position() throws IOException {
        return tempFileChannel.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        tempFileChannel.position(newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        return tempFileChannel.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        modified = true;
        tempFileChannel.truncate(size);
        return this;
    }

    @Override
    public boolean isOpen() {
        return tempFileChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        try {
            tempFileChannel.close();
            if (modified) {
                client.putObject(bucketName, objectKey, tempFile);
            }
        }finally {
            if (!tempFile.delete()) {
                tempFile.deleteOnExit();
            }
        }
    }
}
