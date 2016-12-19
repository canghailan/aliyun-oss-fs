package cc.whohow.fs;

import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.regex.Pattern;

/**
 * 正则表达式路径匹配，提供通用路径匹配功能
 */
public class PatternPathMatcher implements PathMatcher {
    private final Pattern pattern;

    public PatternPathMatcher(Pattern pattern) {
        this.pattern = pattern;
    }

    public PatternPathMatcher(String pattern) {
        this(Pattern.compile(pattern));
    }

    @Override
    public boolean matches(Path path) {
        return pattern.matcher(path.toString()).matches();
    }
}
