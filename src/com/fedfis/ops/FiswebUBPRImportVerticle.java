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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is for importing UBPR to FisWeb. It makes a few assumptions that may not hold for CALL data.
 */
public class FiswebUBPRImportVerticle extends XBRLImportVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FiswebUBPRImportVerticle.class.getName());

    @Override
    protected SimpleFileVisitor<Path> getPublishingVisitor() {
        final String glob = "glob:/*.xml";
        final PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher(glob);
        return new UBPRPublishingVisitor(pathMatcher, vertx.eventBus(), listenerAddress);
    }

    /**
     * Entrypoint for verticle.
     * <p>
     * Setup fisweb (mysql) connection pool, setup eventBus listeners for import to Fisweb
     *
     * @param p
     */
    @Override
    public void start(Promise p) {
        final MySQLConnectOptions connectOptions = new MySQLConnectOptions().setPort(config().getInteger("fiswebConnectionPort", 13306)).setHost(config().getString("fiswebConectionHost", "localhost")).setDatabase(config().getString("fiswebConnectionDb", "fis_common")).setUser(config().getString("fiswebConnectionUser", "admin")).setPassword(config().getString("fiswebConnectionPassword", "HARDCODED_PASSWORD"));
        final MySQLPool fiswebPool = MySQLPool.pool(vertx, connectOptions, new PoolOptions().setMaxSize(8));

        final EventBus eb = vertx.eventBus();

        String dmTable = "b2021q4dm";
        // TODO need table name in here but don't even have a date to work with until later
        String bTable = "b2019q1b";

        List<String> targetColumns = new ArrayList<>();
        Map<Integer, Integer> idrssd_cert = new HashMap<>(5000);

        eb.consumer(busAddress, getURIHandler());

        // fisweb import setup

        // setup: listen for parsed UBPR blocks
        // this is all of the FISWEB import implementation
        eb.consumer(listenerAddress, new FiswebBankImport(fiswebPool, targetColumns, idrssd_cert));

        // setup: load idsrrd and ubprColumns
        fiswebPool.preparedQuery("SELECT rssd9001,id FROM bankdata2017q2." + dmTable).execute().compose(result -> {
            result.forEach(row -> idrssd_cert.put(row.getInteger(0), row.getInteger(1)));
            return Future.succeededFuture();
        }).compose(_foo -> {
            // The constraint on c.COLUMN_NAME means only UBPR will be imported
            return fiswebPool.preparedQuery("SELECT upper(c.column_name) FROM information_schema.columns c\nWHERE c.TABLE_SCHEMA='bankdata2017q2'\n\tAND c.TABLE_NAME = ?\n" + // try not to inject m4lw4r3 into ur own sql
                    "\tAND upper(c.COLUMN_NAME) LIKE 'UBPR%'\nORDER BY c.TABLE_NAME DESC").execute(Tuple.of(bTable));
        }).compose(p_cols -> {
            p_cols.forEach(row -> targetColumns.add(row.getString(0)));
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
    private class FiswebBankImport implements Handler<Message<Object>> {
        private final List<String> targetColumns;
        private final Map<Integer, Integer> idrssd_cert;
        final MySQLPool fiswebPool;

        /**
         * Write data items to fisweb as they come through.
         * TODO: push up handler, detection of flush and ubpr events
         * TODO: push up abstracts for handling flush and ubpr events
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
            final JsonObject ubpr = messageJson.getJsonObject("ubpr");
            final JsonObject divisors = messageJson.getJsonObject("divisors");
            final LocalDate dataDate = LocalDate.parse(ubpr.getString(XBRLImportConfig.UBPR_9999));
            final String query = generateUpdate(dataDate, targetColumns, ubpr, divisors, idrssd_cert, parms);
            fiswebPool.preparedQuery(query).execute(parms);
        }

        public FiswebBankImport(MySQLPool fiswebPool, List<String> targetColumns, Map<Integer, Integer> idrssd_cert) {
            this.fiswebPool = fiswebPool;
            this.targetColumns = targetColumns;
            this.idrssd_cert = idrssd_cert;
        }
    }

    /**
     * Generate a fisweb UPDATE statement and Tuple by iterating through columns, appending assignment placeholders
     * to the query and filling Tuple t with values.
     * <p>
     * This is for banks but could be generalized.
     *
     * @param dataDate Date of table name and content
     * @param columns  List of columns for update
     * @param ubpr     Column values
     * @param idrssd   Map of idrssd:cert
     * @param t        OUT: Tuple of values for update
     * @return query
     */
    public String generateUpdate(LocalDate dataDate, List<String> columns, JsonObject ubpr, JsonObject divisors, Map<Integer, Integer> idrssd, Tuple t) {
        StringBuffer query = new StringBuffer(8196);

        // ERROR LOG we don't even have rssdid in the record
        if (!(columns.size() > 0 && ubpr.containsKey(XBRLImportConfig.RSSD_9001))) {
            logger.error("Missing columns and/or RSSD9001");
            return "";
        } else {
            String tableName = generateTable("b", "b", dataDate);
            query.append("UPDATE bankdata2017q2.").append(tableName).append(" SET ");
            List<String> ihatejavasometimes = new ArrayList<>();
            columns.forEach(col -> {
                if (ubpr.containsKey(col)) {
                    ihatejavasometimes.add(col + "=?/" + divisors.getInteger(col,1));
                    t.addValue(ubpr.getValue(col));
                }
            });

            String setvals = String.join(",", ihatejavasometimes);
            query.append(setvals).append(" WHERE id=?");
            t.addValue(idrssd.get(ubpr.getInteger(XBRLImportConfig.RSSD_9001)));
            return query.toString();
        }
    }

    public FiswebUBPRImportVerticle(String busAddress) {
        super(busAddress);
    }
}
