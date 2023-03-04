package com.fedfis.ops;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * XBRLImportVerticle implements the generic part of loading XBRL into memory for transformation and storage
 * <p>
 * The kickStart() method starts an async loop that attempts to import the XBRL from zip files
 * published as URIs to the busAddress channel.
 */
public abstract class XBRLImportVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(XBRLImportVerticle.class.getName());
    final protected ArrayBlockingQueue<URI> zipQ;

    protected final String listenerAddress = "xbrl.listen";
    protected final String busAddress;
    protected Boolean busy = false;

    public XBRLImportVerticle(String busAddress) {
        this.busAddress = busAddress;
        this.zipQ = new ArrayBlockingQueue<>(100);
    }

    public void kickStart() {
        try {
            busy = true;
            walkFileTree(zipQ.take()).onSuccess(unused -> {
                if (!zipQ.isEmpty()) {
                    kickStart();
                } else {
                    busy = false;
                }
            }).onFailure(t -> {
                logger.error(t.toString());
            });
        } catch (Exception e) {
            logger.debug(e.getMessage());
        }
    }

    @NotNull
    protected Handler<Message<Object>> getURIHandler() {
        return message -> {
            try {
                URI uri = URI.create("jar:" + Path.of(message.body().toString()).toUri());
                logger.debug("Found for import: " + uri);
                zipQ.add(uri);
                if (!busy) kickStart();
            } catch (Exception e) {
                logger.debug(e.getMessage());
            }
        };
    }

    /**
     * Get a file visitor
     * <p>
     * This should extend SimpleFileVisitor<Path>
     */
    protected abstract SimpleFileVisitor<Path> getPublishingVisitor();

    /**
     * Walk the zip filesystem for XML files
     *
     * @param uri Filesystem URI
     */
    @NotNull
    private void walkFileTreeImpl(URI uri) {
        try (FileSystem xbrlFS = FileSystems.newFileSystem(uri, new HashMap<>())) {
            Path zipfsRoot = xbrlFS.getRootDirectories().iterator().next();
            Files.walkFileTree(zipfsRoot, getPublishingVisitor());
        } catch (Exception e) {
            logger.debug(e.toString());
        }
    }

    /**
     * Return a Future of walkFileTreeImpl(uri) using a thread outside the Vert.x pool
     *
     * @param uri location of XBRL files
     * @return Future List of Tuple(rssdid, {ubpr values}, report_date)
     */
    public Future<Void> walkFileTree(URI uri) {
        ExecutorService es = Executors.newSingleThreadExecutor();
        return Future.fromCompletionStage(CompletableFuture.runAsync(() -> walkFileTreeImpl(uri), es));
    }

    /**
     * Generate a fisweb table name from institution type, table type, and date
     *
     * @param iType    String containing the fisweb institution type prefix, one of: b c h r
     * @param tType    String containing the fisweb table type suffix, one of: a b calc dm pa pc pq rankingq ratingy rankingq rankingy
     * @param dataDate Date (quarter) for the table
     * @return Unqualified table name
     */
    public String generateTable(String iType, String tType, LocalDate dataDate) {
        if (!(iType.equals("b") || iType.equals("c") || iType.equals("h") || iType.equals("r"))) {
            logger.warn("Generating institution type [" + iType + "] for " + dataDate.format(DateTimeFormatter.ofPattern("q")) + " " + tType + " table ");
        }
        if (!(tType.equals("a") || tType.equals("b") || tType.equals("calc") || tType.equals("dm") || tType.equals("pa") || tType.equals("pc") || tType.equals("pq") || tType.equals("rankingq") || tType.equals("rankingy") || tType.equals("ratingq") || tType.equals("ratingy"))) {
            logger.warn("Generating table type [" + tType + "] for " + iType + dataDate.format(DateTimeFormatter.ofPattern("q")));
        }
        return iType + dataDate.getYear() + "q" + dataDate.format(DateTimeFormatter.ofPattern("q")) + tType;
    }

}
