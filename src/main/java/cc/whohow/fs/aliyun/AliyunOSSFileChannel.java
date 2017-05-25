package cc.whohow.fs.aliyun;

import com.aliyun.oss.model.GetObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;

public class AliyunOSSFileChannel implements SeekableByteChannel {
    private final AliyunOSSPath path; // 路径
    private final File tempFile; // 临时文件
    private final FileChannel tempFileChannel; // 临时文件Channel
    private volatile boolean modified; // 文件是否被修改

    public AliyunOSSFileChannel(AliyunOSSPath path) throws IOException {
        if (!path.isFile()) {
            throw new IOException(path.toUri() + " is not file");
        }
        this.path = path;
        this.tempFile = File.createTempFile("AliyunOSSFileChannel", null);
        if (this.path.getClient().doesObjectExist(path.getBucketName(), path.getObjectKey())) {
            this.path.getClient().getObject(new GetObjectRequest(path.getBucketName(), path.getObjectKey()), this.tempFile);
        }
        this.tempFileChannel = FileChannel.open(tempFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.modified = false;
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
                path.getClient().putObject(path.getBucketName(), path.getObjectKey(), tempFile);
            }
        }finally {
            if (!tempFile.delete()) {
                tempFile.deleteOnExit();
            }
        }
    }
}
