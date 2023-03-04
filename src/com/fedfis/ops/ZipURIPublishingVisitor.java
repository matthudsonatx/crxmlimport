package com.fedfis.ops;

import io.vertx.core.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class ZipURIPublishingVisitor extends SimpleFileVisitor<Path> {
    private static final Logger logger = LoggerFactory.getLogger(ZipURIPublishingVisitor.class.getName());
    protected final PathMatcher pathMatcher;
    protected final EventBus eventBus;
    protected final String zipChannel;

    public ZipURIPublishingVisitor(PathMatcher pathMatcher, EventBus eventBus, String zipChannel) {
        this.pathMatcher = pathMatcher;
        this.eventBus = eventBus;
        this.zipChannel = zipChannel;
    }

    @Override
    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
        if (path.toFile().isFile() && pathMatcher.matches(path)) {
            final String s = path.toString();
            eventBus.publish(zipChannel, path.toString());
        } else {
            logger.info("Skipping: " + path);
        }

        return FileVisitResult.CONTINUE;
    }
}
