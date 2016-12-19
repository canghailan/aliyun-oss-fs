package cc.whohow.fs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.util.Iterator;
import java.util.stream.StreamSupport;

/**
 * 目录文件流过滤
 */
public class FilterDirectoryStream<T> implements DirectoryStream<T> {
    private final DirectoryStream<T> directoryStream;
    private final DirectoryStream.Filter<? super T> filter;

    public FilterDirectoryStream(DirectoryStream<T> directoryStream, Filter<? super T> filter) {
        this.directoryStream = directoryStream;
        this.filter = filter;
    }

    @Override
    public Iterator<T> iterator() {
        return StreamSupport.stream(directoryStream.spliterator(), false)
                .filter(self -> {
                    try {
                        return filter.accept(self);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .iterator();
    }

    @Override
    public void close() throws IOException {
        directoryStream.close();
    }
}
