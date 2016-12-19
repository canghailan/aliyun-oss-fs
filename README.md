# 阿里云OSS Java FileSystem API

## Java FileSystem API

* [FileSystem](https://docs.oracle.com/javase/7/docs/api/java/nio/file/FileSystem.html)
* [Path](https://docs.oracle.com/javase/7/docs/api/java/nio/file/Path.html)



## 阿里云OSS

* [Bucket、Object、Endpoint](https://help.aliyun.com/document_detail/31827.html)
* OSSClient：阿里云OSS SDK客户端



## 实现

### 1. 配置文件

```properties
# 阿里云AccessKeyId
accessKeyId=
# 阿里云SecretAccessKey
secretAccessKey=
# 外网Endpoint，如华东1（杭州）
endpoint=oss-cn-hangzhou.aliyuncs.com
# 内网Endpoint，如华东1（杭州）
endpoint-internal=oss-cn-hangzhou-internal.aliyuncs.com
# Bucket
bucketName=xxx

# 文件系统配置，可定义多个，通过前缀区分
# 虚拟文件路径
test.vfs=/vfs-test/
# 覆盖全局同名配置
test.bucketName=xxx
# OSS公共前缀
test.prefix=test/
# CDN域名
test.cname=xxx.img-cn-hangzhou.aliyuncs.com
```

### 2. 初始化

```java
try (AliyunOSSFileSystemProvider fs = new AliyunOSSFileSystemProvider(properties)) {
    
}
```

### 3. 获取Path对象

```java
// 通过虚拟文件路径
AliyunOSSPath path = fs.getPath("/vfs-test/");
// 通过OSS标准URI
AliyunOSSPath path = fs.getPath("http://xxx.oss-cn-hangzhou.aliyuncs.com/test/");
// 通过URL
AliyunOSSPath path = fs.getPath("http://xxx.img-cn-hangzhou.aliyuncs.com/test/");
```

### 4. 基础API

```java
// 拷贝，URL上传
void copy(URL source, AliyunOSSPath target)
// 拷贝，文件上传
void copy(File source, AliyunOSSPath target)
// 拷贝，下载
void copy(AliyunOSSPath source, File target)
// 拷贝，流上传
void copy(InputStream source, AliyunOSSPath target)
// 拷贝
void copy(AliyunOSSPath source, AliyunOSSPath target)
// 拷贝文件夹，上传文件夹
void copyRecursively(File source, AliyunOSSPath target)
// 拷贝文件夹，下载文件夹
void copyRecursively(AliyunOSSPath source, File target)
// 拷贝文件夹
void copyRecursively(AliyunOSSPath source, AliyunOSSPath target)
// 删除文件
void delete(AliyunOSSPath path)
// 删除文件夹
void deleteRecursively(AliyunOSSPath path)
// 移动（重命名）文件
void move(AliyunOSSPath source, AliyunOSSPath target)
// 移动（重命名）文件夹
void moveRecursively(AliyunOSSPath source, AliyunOSSPath target)
// 获取文件元数据
ObjectMetadata getMetadata(AliyunOSSPath path)
// 获取文件、文件夹大小
long getSize(AliyunOSSPath path)
// 获取文件夹下文件数
long getCount(AliyunOSSPath path)
// 获取文件、文件夹最后修改时间
Date getLastModified(AliyunOSSPath path)
// 读取文件流
InputStream newInputStream(AliyunOSSPath path)
// 子文件、文件夹列表，OSS标准URI字符串格式
Iterable<String> list(AliyunOSSPath path)
// 子文件、文件夹列表
Iterable<AliyunOSSPath> listPath(AliyunOSSPath path)
// 子文件、文件夹列表
Iterable<OSSObjectSummary> listObjects(AliyunOSSPath path)
// 递归子文件、文件夹列表，OSS标准URI字符串格式
Iterable<String> listRecursively(AliyunOSSPath path)
// 递归子文件、文件夹列表
Iterable<AliyunOSSPath> listPathRecursively(AliyunOSSPath path)
// 递归子文件、文件夹列表
Iterable<OSSObjectSummary> listObjectsRecursively(AliyunOSSPath path)
```

### 5. 例子

```java
public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    try (InputStream props = Thread.currentThread().getContextClassLoader().getResourceAsStream("aliyun-oss.properties")) {
        properties.load(props);
    }
    try (AliyunOSSFileSystemProvider fs = new AliyunOSSFileSystemProvider(properties)) {
        System.out.println(fs.getPath("/vfs-test/")); // 虚拟文件路径
        fs.list("/vfs-test/").forEach(System.out::println);

        System.out.println(fs.getPath("http://xxx.oss-cn-hangzhou.aliyuncs.com/test/")); // OSS标准URI
        fs.list("http://xxx.oss-cn-hangzhou.aliyuncs.com/test/").forEach(System.out::println);

        System.out.println(fs.getPath("http://xxx.img-cn-hangzhou.aliyuncs.com/test/")); // URL
        fs.list("http://xxx.img-cn-hangzhou.aliyuncs.com/test/").forEach(System.out::println);
    }
}
```

