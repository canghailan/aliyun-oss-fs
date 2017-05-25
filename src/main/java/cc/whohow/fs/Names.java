package cc.whohow.fs;

import java.util.Arrays;
import java.util.UUID;

public class Names {
    /**
     * 路径名数量
     */
    public static int getNameCount(String names) {
        int count = 1;
        int from = names.startsWith("/") ? 1 : 0;
        int to = names.endsWith("/") ? names.length() - 1 : names.length();
        for (int i = from; i < to; i++) {
            if (names.charAt(i) == '/') {
                count++;
            }
        }
        return count;
    }

    /**
     * 路径名列表
     */
    public static Iterable<String> getNames(String names) {
        return Arrays.asList(names.replaceAll("(^/|/$)", "").split("/"));
    }

    /**
     * 名称
     */
    public static String getName(String uri) {
        int end = uri.endsWith("/") ? uri.length() - 1 : uri.length();
        int begin = uri.lastIndexOf('/', end - 1);
        return uri.substring(begin + 1, end);
    }

    /**
     * 上级路径
     */
    public static String getPrefix(String uri) {
        int end = uri.endsWith("/") ?
                uri.lastIndexOf('/', uri.length() - 2) :
                uri.lastIndexOf('/');
        if (end < 0) {
            return "";
        } else {
            return uri.substring(0, end + 1);
        }
    }

    /**
     * 后缀名
     */
    public static String getSuffix(String uri) {
        if (uri.endsWith("/")) {
            return null;
        }
        String name = getName(uri);
        int begin = name.lastIndexOf('.');
        if (begin < 0) {
            return null;
        }
        return name.substring(begin);
    }

    /**
     * 随机名称
     */
    public static String newRandomName(String names) {
        return newRandomNames("", getSuffix(names));
    }

    /**
     * 随机路径
     */
    public static String newRandomNames(String names) {
        return newRandomNames(getPrefix(names), names.endsWith("/") ? "/" : getSuffix(names));
    }

    /**
     * 随机路径
     */
    public static String newRandomNames(String prefix, String suffix) {
        if (prefix == null) {
            throw new IllegalArgumentException();
        }
        if (suffix == null) {
            return prefix + UUID.randomUUID();
        } else {
            return prefix + UUID.randomUUID() + suffix;
        }
    }
}
