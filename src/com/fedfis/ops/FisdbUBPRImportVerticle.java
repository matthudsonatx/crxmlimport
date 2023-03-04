package com.fedfis.ops;

import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static com.fedfis.ops.XBRLImportConfig.RSSD_9001;


public class FisdbUBPRImportVerticle extends XBRLImportVerticle {
    final private static Logger logger = LoggerFactory.getLogger(FisdbUBPRImportVerticle.class.getName());
    protected static final int QSIZE = 1000;
    protected PgPool fisdbPool;
    final protected ArrayBlockingQueue<Tuple> insertQ;

    final protected String ins_query = "INSERT INTO scratchpad.ubpr (rssd_id, ubpr, reported_on) VALUES ($1, $2, $3) ON CONFLICT (rssd_id, reported_on) DO UPDATE SET ubpr=EXCLUDED.ubpr";

    @Override
    protected SimpleFileVisitor<Path> getPublishingVisitor() {
        final String glob = "glob:/*.xml";
        final PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(glob);
        return new UBPRPublishingVisitor(pathMatcher, vertx.eventBus(), listenerAddress);
    }

    public FisdbUBPRImportVerticle(String busAddress) {
        super(busAddress);
        this.insertQ = new ArrayBlockingQueue<>(QSIZE);
    }

    @Override
    public void start(Promise p) {
        fisdbPool = PgPool.pool(vertx, config().getString(XBRLImportConfig.CFG_PGCONNECTIONURI), new PoolOptions().setMaxSize(8));

        final EventBus eb = vertx.eventBus();

        // setup: listen for URIs
        eb.consumer(busAddress, getURIHandler());

        // setup: listen for UBPR messages
        eb.consumer(listenerAddress, message -> {
            final JsonObject messageJson = new JsonObject(message.body().toString());

            if (messageJson.getBoolean("flush", false)) {
                flushQueue();
            } else {
                final JsonObject ubpr = messageJson.getJsonObject("ubpr");
                final int rssd_id = ubpr.getInteger(RSSD_9001);
                final LocalDate dataDate = LocalDate.parse(ubpr.getString(XBRLImportConfig.UBPR_9999));
                Tuple t = Tuple.of(rssd_id, ubpr, dataDate);
                insertQ.add(t);

                if (insertQ.remainingCapacity() == 0) {
                    flushQueue();
                }
            }
        });

        p.complete();
    }

    private void flushQueue() {
        if (insertQ.size() > 0) {
            List<Tuple> batch = new ArrayList<>(insertQ.size());
            insertQ.drainTo(batch);
            logger.debug("INSERT " + batch.size());
            fisdbPool.preparedQuery(ins_query).executeBatch(batch).onFailure(fail -> {
                logger.error("INSERT failure: " + fail.toString());
                fail.printStackTrace();
            });
        }
    }
}
