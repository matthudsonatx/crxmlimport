package com.fedfis.ops;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.codehaus.stax2.XMLInputFactory2;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UBPRPublishingVisitor extends SimpleFileVisitor<Path> {
    private static final Logger logger = LoggerFactory.getLogger(UBPRPublishingVisitor.class.getName());
    protected static final Pattern xbrlPattern = Pattern.compile(".* (\\d+)\\(ID RSSD\\) (\\d+).XBRL.xml");
    protected final String xbrlBusAddress;
    protected final PathMatcher pathMatcher;
    protected final EventBus eventBus;

    private boolean learnContextRefId;

    /**
     * A Path visitor that parses UBPR data from XML documents and publishes a JSON message for each document.
     *
     * @param pathMatcher
     * @param eventBus
     * @param xbrlBusAddress
     */
    public UBPRPublishingVisitor(PathMatcher pathMatcher, EventBus eventBus, String xbrlBusAddress) {
        this.pathMatcher = pathMatcher;
        this.eventBus = eventBus;
        this.xbrlBusAddress = xbrlBusAddress;
        this.setLearnContextRefId(false);
    }

    @Fluent
    public void setLearnContextRefId(boolean learnContextRefId) {
        this.learnContextRefId = learnContextRefId;
    }

    public boolean getLearnContextRefId() {
        return learnContextRefId;
    }

    /**
     * Filter XBRL files from zip file, load all identifiers from Concepts and SourceConcepts namespaces
     *
     * @param path  Path of the visited file. This method filters by the naming convention the FFIEC-sourced files use.
     * @param attrs Attributes of the visited file (unused)
     * @return Always CONTINUE; nothing needs to prevent the whole directory from being scanned.
     */
    @Override
    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
        Matcher m = xbrlPattern.matcher(path.getFileName().toString());
        if (pathMatcher.matches(path) && m.find()) {
            final String fileReportedOn = m.group(2);
            final LocalDate reportedOn = LocalDate.parse(fileReportedOn, DateTimeFormatter.ofPattern("MMddyyyy"));
            final JsonObject result = new JsonObject();
            final JsonObject divisors = new JsonObject();
            final JsonObject ubpr = getEntries(path, reportedOn, divisors);
            result.put("ubpr", ubpr);
            result.put("divisors", divisors);
            result.put("reported_on", reportedOn.format(DateTimeFormatter.ISO_DATE));
            result.put("path", path);
            eventBus.publish(xbrlBusAddress, result.encode());
        } else {
            logger.info("Skipping: " + path);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException ioe) {
        eventBus.publish(xbrlBusAddress, "{\"flush\": true}");
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
        return FileVisitResult.CONTINUE;
    }

    /**
     * Read through files given by ubprPath for the UBPR data comprising Concepts and SourceConcepts namespaces
     *
     * @param ubprPath   Path of XBRL file
     * @param reportedOn Report date of XBRL file
     * @return JsonObject containing entries for UBPR variables
     */
    @NotNull
    public JsonObject getEntries(Path ubprPath, LocalDate reportedOn, JsonObject divisors) {
        final JsonObject jsonObject = new JsonObject();
        final String contextRef = reportedOn.format(DateTimeFormatter.ISO_DATE);

        XMLStreamReader xmlStreamReader;
        try {
            xmlStreamReader = XMLInputFactory2.newInstance().createXMLStreamReader(Files.newInputStream(ubprPath));

            while (xmlStreamReader.hasNext()) {
                xmlStreamReader.next();
                xmlStreamReader.nextTag();

                // contextRef is the 0th attribute
                // until it's not
                int contextRefId = 0;
                // unitRef is the 1th attribute
                // until it's not
                int unitRefId = 1;
                if (learnContextRefId) {
                    for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                        final String attrName = xmlStreamReader.getAttributeName(i).toString();
                        if (attrName.equals("contextRef")) {
                            contextRefId = i;
                            break;
                        }
                    }
                }

                // several namespaces contain the variables we want
                // tag must also contain a contextRef attribute that ends with target filing date
                if ((xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v121/Concepts") ||
                        xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v121/SourceConcepts") ||
                        xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v113/Concepts") ||
                        xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v113/SourceConcepts") ||
                        xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v116/Concepts") ||
                        xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v116/SourceConcepts") ||
                        xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v117/Concepts") ||
                        xmlStreamReader.getNamespaceURI().equals("http://www.cdr.ffiec.gov/xbrl/ubpr/v117/SourceConcepts")) &&
                        xmlStreamReader.getAttributeValue(contextRefId).endsWith(contextRef)) {

                    String unit = "";
                    try {
                        if (xmlStreamReader.getEventType() == XMLStreamConstants.START_ELEMENT) {
                            if (xmlStreamReader.getAttributeCount() > unitRefId) {
                                unit = xmlStreamReader.getAttributeValue(unitRefId);
                            }
                        }
                    } catch (Exception e) {
                        logger.debug(e.getMessage());
                    }

                    final String name = xmlStreamReader.getLocalName();
                    final String text = xmlStreamReader.getElementText();
                    if (unit.equals("USD")) {
                        divisors.put(name, 1000);
                    }

                    if (text.equals("true") || text.equals("false")) {
                        // Boolean.parseBoolean returns false unless it is passed "true"
                        // it doesn't throw parser exceptions
                        jsonObject.put(name, Boolean.parseBoolean(text));
                    } else {
                        try {
                            // attempt int
                            jsonObject.put(name, Integer.parseInt(text));
                        } catch (Exception e1) {
                            try {
                                // not int, try double
                                jsonObject.put(name, Double.parseDouble(text));
                            } catch (Exception e2) {
                                // not double, store quoted
                                jsonObject.put(name, text);
                            }
                        }
                    }
                }
            }
        } catch (XMLStreamException | NoSuchElementException xse) {

        } catch (Exception e) {

        }
        return jsonObject;
    }
}