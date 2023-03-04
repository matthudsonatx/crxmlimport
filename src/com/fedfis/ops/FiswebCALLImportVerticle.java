package com.fedfis.ops;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * This is for importing call reports to FisWeb
 * <p>
 * TODO everything
 */
public class FiswebCALLImportVerticle extends XBRLImportVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FiswebCALLImportVerticle.class.getName());

    @Override
    protected SimpleFileVisitor<Path> getPublishingVisitor() {
        final String glob = "glob:/*.xml";
        final PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(glob);
        return new CALLPublishingVisitor(pathMatcher, vertx.eventBus(), listenerAddress);
    }

    /**
     * Entrypoint for verticle.
     * <p>
     * Setup fisweb (mysql) connection pool, setup eventBus listeners to import call reports to Fisweb
     *
     * @param p
     */
    @Override
    public void start(Promise p) {
        final MySQLConnectOptions connectOptions = new MySQLConnectOptions().setPort(config().getInteger("fiswebConnectionPort", 13306)).setHost(config().getString("fiswebConectionHost", "localhost")).setDatabase(config().getString("fiswebConnectionDb", "fis_common")).setUser(config().getString("fiswebConnectionUser", "admin")).setPassword(config().getString("fiswebConnectionPassword", "HARDCODED_PASSWORD"));
        final MySQLPool fiswebPool = MySQLPool.pool(vertx, connectOptions, new PoolOptions().setMaxSize(8));

        final EventBus eb = vertx.eventBus();

        List<String> targetColumns = new ArrayList<>();

        List<String> importOnlyColumns = new ArrayList<>();
        importOnlyColumns.add("RIADB947");
        importOnlyColumns.add("RIADB947");
        importOnlyColumns.add("RIADB948");
        importOnlyColumns.add("RIADB949");
        importOnlyColumns.add("RIADB950");
        importOnlyColumns.add("RIADB951");
        importOnlyColumns.add("RIADB952");
        importOnlyColumns.add("RIADB953");
        importOnlyColumns.add("RIADB954");
        importOnlyColumns.add("RIADB955");
        importOnlyColumns.add("RIADB956");
        importOnlyColumns.add("RIADB957");
        importOnlyColumns.add("RIADB958");
        importOnlyColumns.add("RIADB959");
        importOnlyColumns.add("RIADB960");
        importOnlyColumns.add("RIADB961");

        eb.consumer(busAddress, getURIHandler());

        // fisweb import setup

        // setup: listen for parsed UBPR blocks
        // this is all of the FISWEB import implementation
        // TODO update FiswebBankImport to something more appropriate here
        eb.consumer(listenerAddress, new FiswebCallImportHandler(fiswebPool, targetColumns));

        fiswebPool.preparedQuery("SELECT upper(c.column_name) FROM information_schema.columns c\nWHERE c.TABLE_SCHEMA='bankdata2017q2'\n\tAND c.TABLE_NAME = ?\n" + // try not to inject m4lw4r3 into ur own sql
                        "ORDER BY c.TABLE_NAME DESC").execute(Tuple.of("b2021q4b"))
                .compose(p_cols -> {
                    p_cols.forEach(row -> {
                        String candidateCol = row.getString(0);
                        // TODO Hardcoded: Limit attributes to importOnlyColumns
                        if (importOnlyColumns.contains(candidateCol)) {
                            logger.debug("Adding " + candidateCol + " for import");
                            targetColumns.add(candidateCol);
                        }
                    });
                    p.complete();
                    return Future.succeededFuture();
                }).onFailure(t -> {
                    logger.error(t.getMessage());
                    p.fail(t);
                });
    }

    /**
     * This could be absorbed into start() but it made some sense to isolate it here
     */
    private class FiswebCallImportHandler implements Handler<Message<Object>> {
        private final List<String> targetColumns;
        final MySQLPool fiswebPool;

        /**
         * Write data items to fisweb as they come through.
         * TODO: push up handler, detection of flush and call events
         * TODO: push up abstracts for handling flush and call events
         *
         * @param message
         */
        @Override
        public void handle(Message message) {
            final Tuple parms = Tuple.tuple();
            final JsonObject messageJson = new JsonObject(message.body().toString());
            if (messageJson.getBoolean("flush", false)) {
                return;
            }
            try {
                final JsonObject call = messageJson.getJsonObject("call");
                final JsonObject divisors = messageJson.getJsonObject("divisors");
                final LocalDate dataDate = LocalDate.parse(call.getString(XBRLImportConfig.RCON_9999), DateTimeFormatter.ofPattern("yyyyMMdd"));
                final String query = generateUpdate(dataDate, targetColumns, call, divisors, parms);
                if (query.length() > 0) {
                    fiswebPool.preparedQuery(query).execute(parms).onFailure(t -> {
                        logger.error("Query failed: " + t);
                    });
                }
            } catch (Exception e) {
                logger.error("Proccessing " + messageJson.getString("path"));
                logger.error("Caught: " + e.toString());
                logger.error("Message: " + e.getMessage());
            }
        }

        public FiswebCallImportHandler(MySQLPool fiswebPool, List<String> targetColumns) {
            this.fiswebPool = fiswebPool;
            this.targetColumns = targetColumns;
        }
    }

    /**
     * Generate a fisweb UPDATE statement and Tuple by iterating through columns, appending assignment placeholders
     * to the query and filling Tuple t with values.
     * <p>
     * This is for banks but could be generalized.
     *
     * @param dataDate   Date of table name and content
     * @param columns    List of columns for update
     * @param callReport Column values
     * @param t          OUT: Tuple of values for update
     * @return query
     */
    public String generateUpdate(LocalDate dataDate, List<String> columns, JsonObject callReport, JsonObject divisors, Tuple t) {
        StringBuffer query = new StringBuffer(8196);

        // ERROR LOG we don't even have rssdid in the record
        if (!(columns.size() > 0 && callReport.containsKey(XBRLImportConfig.RSSD_9050))) {
            logger.error("Missing columns and/or RSSD9050");
            return "";
        } else {
            // TODO at time of writing only columns in B table need to be updated
            // TODO OBVIOUSLY THIS IS A HARDCODED HACK TO BE FIXED IF THIS CODE NEEDS MORE THAN 1 ROLE
            String tableName = generateTable("b", "b", dataDate);
            query.append("UPDATE bankdata2017q2.").append(tableName).append(" SET ");
            List<String> ihatejavasometimes = new ArrayList<>();
            columns.forEach(col -> {
                if (callReport.containsKey(col)) {
                    ihatejavasometimes.add(col + "=?/" + divisors.getInteger(col, 1));
                    t.addValue(callReport.getValue(col));
                }
            });

            String setvals = String.join(",", ihatejavasometimes);
            query.append(setvals).append(" WHERE id=?");
            t.addValue(callReport.getInteger(XBRLImportConfig.RSSD_9050));
            if (ihatejavasometimes.size() == 0) {
                return "";
            }
            return query.toString();
        }
    }

    public FiswebCALLImportVerticle(String busAddress) {
        super(busAddress);
    }
}
