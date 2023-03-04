package com.fedfis.ops;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;

public class Launcher {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Launcher.class.getName());

        Vertx vertx = Vertx.vertx();

        boolean p_launchFisdbImport = false;
        boolean p_launchFiswebImport = false;
        boolean p_launchFiswebCallImport = false;

//        CommandBuilder builder = CommandBuilder.command("import-ubpr-files");
//        builder.processHandler(process -> {
//
//            for (String arg : process.args()) {
//                // Print each argument on the console
//                process.write("Argument " + arg);
//                logger.info("Argument " + arg);
//            }
//
//            // End the process
//            process.end();
//        });
//        // Register the command
//        CommandRegistry registry = CommandRegistry.getShared(vertx);
//        registry.registerCommand(builder.build(vertx));

        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            final int equalsAt = arg.indexOf('=');
            final String rvalue = equalsAt > -1 ? arg.substring(equalsAt + 1) : "";
            final String lvalue = equalsAt > -1 ? arg.substring(0, equalsAt) : arg;

            switch (lvalue) {
                case "fisdb":
                    logger.info(lvalue);
                    p_launchFisdbImport = true;
                    break;
                case "fisweb":
                    logger.info(lvalue);
                    p_launchFiswebImport = true;
                    break;
                case "fiswebcall":
                    logger.info(lvalue);
                    p_launchFiswebCallImport = true;
                default:
                    break;
            }
        }

        final boolean launchFisdbImport = p_launchFisdbImport;
        final boolean launchFiswebImport = p_launchFiswebImport;
        final boolean launchFiswebCallImport = p_launchFiswebCallImport;
        final String uriAddress = "xbrl.uri.listen";// publish file:jar:/f/ffiec/FFIEC ... .XBRL.zip

        // TODO move this launcher into a verticle, launch with vertx cli
        ConfigRetriever.create(vertx)
                .getConfig(json -> {
                    JsonObject result = json.result();
                    String env = System.getenv("FISBOX");
                    env = env == null ? "fisdev" : env;// default config: matt's laptop
                    JsonObject config = result
                            .getJsonObject(env);
                    config.put("FISBOX", env);

                    Future<String> f = Future.succeededFuture();

                    if (launchFisdbImport)
                        f = vertx.deployVerticle(new FisdbUBPRImportVerticle(uriAddress), new DeploymentOptions().setConfig(config));
                    if (launchFiswebImport)
                        f = vertx.deployVerticle(new FiswebUBPRImportVerticle(uriAddress), new DeploymentOptions().setConfig(config));
                    if (launchFiswebCallImport)
                        f = vertx.deployVerticle(new FiswebCALLImportVerticle(uriAddress), new DeploymentOptions().setConfig(config));

                    // TODO redesign
                    // launch each verticle with separate bus address stem
                    // each verticle adds (standard) channel names to address stem to derive CALL/UBPR/XBRL ZIP addresses
                    //
                    f.compose(verticleId -> {
                        try (FileSystem fs = FileSystems.getDefault()) {
                            // TODO WARNING this config is entirely out of hand -- watch out
                            final String path = launchFiswebCallImport ? config.getString("callPath") : config.getString("ubprPath");
                            final String pathGlob = "glob:" + path + "/*.zip";
                            final PathMatcher pathMatcher = fs.getPathMatcher(pathGlob);
                            final Path fsroot = fs.getPath(path);
                            Files.walkFileTree(fsroot, new ZipURIPublishingVisitor(pathMatcher, vertx.eventBus(), uriAddress));
                        } catch (Exception e) {
                            return Future.failedFuture("Couldn't publish ZIP URI: " + e.toString());
                        }
                        return Future.succeededFuture();
                    }).onFailure(throwable -> {
                        logger.error(throwable.getMessage());
                    });
                });
    }

}
