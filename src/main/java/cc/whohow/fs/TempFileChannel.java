package cc.whohow.fs;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.OpenOption;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * 临时文件Channel，为远程文件提供随机读写功能
 */
public class TempFileChannel implements SeekableByteChannel {
    private final File tempFile; // 临时文件
    private final FileChannel tempFileChannel; // 临时文件Channel
    private final BiFunction<File, Boolean, Void> closeHook; // 关闭钩子
    private volatile boolean modified; // 文件是否被修改

    public TempFileChannel(File tempFile, Set<? extends OpenOption> options, BiFunction<File, Boolean, Void> closeHook) throws IOException {
        this.tempFile = tempFile;
        this.tempFileChannel = FileChannel.open(tempFile.toPath(), options);
        this.closeHook = closeHook;
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
        tempFileChannel.close();
        if (closeHook != null) {
            closeHook.apply(tempFile, modified);
        }
    }
}
