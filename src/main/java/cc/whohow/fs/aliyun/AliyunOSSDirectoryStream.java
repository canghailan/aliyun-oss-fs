package cc.whohow.fs.aliyun;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * 目录流(递归遍历所有子目录及文件)
 */
public class AliyunOSSDirectoryStream implements DirectoryStream<Path> {
    private final AliyunOSSPath directory;

    public AliyunOSSDirectoryStream(AliyunOSSPath directory) {
        if (directory.isFile()) {
            throw new IllegalArgumentException();
        }
        this.directory = directory;
    }

    @Override
    public Iterator<Path> iterator() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new AliyunOSSObjectSummaryIterator(directory.getClient(), directory.getBucketName(), directory.getObjectKey()), 0), false)
                .map(self -> (Path) new AliyunOSSPath(directory.getFileSystem(),"/" + self.getKey()))
                .iterator();
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
