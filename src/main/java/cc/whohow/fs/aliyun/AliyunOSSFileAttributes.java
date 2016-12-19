package cc.whohow.fs.aliyun;

import com.aliyun.oss.model.ObjectMetadata;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;

/**
 * OSS文件属性
 */
public class AliyunOSSFileAttributes implements FileAttributeView, BasicFileAttributes {
    private final AliyunOSSPath path;
    private final ObjectMetadata objectMetadata;

    public AliyunOSSFileAttributes(AliyunOSSPath path) {
        this.path = path;
        this.objectMetadata = path.getClient().getObjectMetadata(path.getBucketName(), path.getObjectKey());
    }

    @Override
    public String name() {
        return ObjectMetadata.class.toString();
    }

    @Override
    public FileTime lastModifiedTime() {
        return FileTime.fromMillis(objectMetadata.getLastModified().getTime());
    }

    @Override
    public FileTime lastAccessTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileTime creationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRegularFile() {
        return path.isFile();
    }

    @Override
    public boolean isDirectory() {
        return !path.isFile();
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return !path.isFile();
    }

    @Override
    public long size() {
        return objectMetadata.getContentLength();
    }

    @Override
    public Object fileKey() {
        return path.toUri();
    }

    @Override
    public String toString() {
        return objectMetadata.toString();
    }
}
