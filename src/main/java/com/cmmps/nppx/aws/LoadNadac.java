package com.cmmps.nppx.aws;

import com.cmmps.nppx.aws.common.AwsApp;
import com.cmmps.nppx.aws.common.AwsUtil;
import com.cmmps.nppx.aws.common.exception.NppxAwsException;
import com.cmmps.nppx.common.Util;
import com.cmmps.nppx.common.model.persistable.DrugPrice;
import com.cmmps.nppx.common.model.persistable.DrugPricePost;
import com.cmmps.nppx.common.model.persistable.Wholesaler;
import com.cmmps.nppx.persistence.HibernateUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class LoadNadac extends AwsApp {
    private static final Logger log = LogManager.getLogger(LoadNadac.class);

    private static final String FILE_KEY = "database/data/NADAC2022.csv";
    private static final int INITIAL_NDC_MAP_CAPACITY = 10000;
    private static final int COMMIT_BOUNDARY = 10000;
    private static final int SAVE_WARN_THRESHOLD = 11;

    public LoadNadac() throws NppxAwsException {
        super();
    }

    public static void main(String[] args) throws Exception {
        var loadNadac = new LoadNadac();

        log.info("Initializing NADAC Started......");

        loadNadac.loadNADAC();

        log.info("Initializing NADAC Complete......");
    }

    public void loadNADAC() throws Exception {
        var startTime = System.currentTimeMillis();
        final var itemsSaved = new Integer[]{0};
        BufferedReader csvFile = null;
        var header = new String[] {"NDC Description", "NDC", "NADAC_Per_Unit", "Effective_Date", "Pricing_Unit",
                                   "Pharmacy_Type_Indicator", "OTC", "Explanation_Code",
                                   "Classification_for_Rate_Setting",
                                   "Corresponding_Generic_Drug_NADAC_Per_Unit",
                                   "Corresponding_Generic_Drug_Effective_Date",
                                   "As of Date"};
        var sdf = new SimpleDateFormat("MM/dd/yyyy");
        Wholesaler wholesaler = HibernateUtil.getByNameAtomic(Wholesaler.class, "nadac");

        if (wholesaler != null) {
            log.info("Data will be posted on behalf of wholesaler %s [%s].".formatted(wholesaler.getName(), wholesaler.getId()));

            var post = new DrugPricePost();

            post.setWholesalerId(wholesaler.getId());
            post.setDefaultEffectiveDate(System.currentTimeMillis());

            final Session session = HibernateUtil.startSession();

            try {
                GetObjectRequest req = GetObjectRequest.builder().key(FILE_KEY).bucket(AwsApp.NPPX_S3_BUCKET_NAME).build();

                var streamReader = new InputStreamReader(AwsUtil.getS3Client(getRegion()).getObjectAsBytes(req).asInputStream());
                csvFile = new BufferedReader(streamReader);

                var csvFormat = CSVFormat.DEFAULT.builder().setHeader(header).setSkipHeaderRecord(true).build();

                // Keyed by NDC description. Try to find NDCs with the same description so a NPD ID can be assigned;
                // same drug different NDCs yields the same NPD ID.
                final var ndcNpdMap = new HashMap<String, String>(INITIAL_NDC_MAP_CAPACITY, 1);

                log.info("Creating Pricing Post...");
                var postId = HibernateUtil.saveAtomic(post);

                csvFormat.parse(csvFile).stream().map(r -> {
                    // some descriptions are wrapped with double quotes
                    var description = StringUtils.stripEnd(StringUtils.stripStart(r.get(header[0]).trim(),
                                                                                  "\""), "\"");

                    // create an ID for each unique NDC
                    if (!ndcNpdMap.containsKey(description)) {
                        ndcNpdMap.put(description, Util.generateId());
                    }

                    // map keys: ndc, price, effDate, uom, npdId, description
                    return Map.of("ndc", r.get(header[1]).trim(), "price", r.get(header[2]).trim(),
                                  "effDate", r.get(header[3]).trim(), "uom", r.get(header[4]).trim(),
                                  "npdId", ndcNpdMap.get(description), "description", description);
                }).map(item -> {
                    var ndc = item.get("ndc");
                    var npdId = item.get("npdId");
                    var description = item.get("description");
                    var uom = item.get("uom");

                    long effDate = 0;
                    try {
                        effDate = (sdf.parse(item.get("effDate"))).getTime();
                    }
                    catch (ParseException pe) {
                        log.error("Failed to convert %s to date.".formatted(item.get("effDate")), pe);
                    }

                    var price = Double.parseDouble(item.get("price"));

                    DrugPrice dp = null;
                    try {
                        dp = new DrugPrice(ndc, description, npdId, price, uom, "undefined", effDate);
                    }
                    catch (Exception dpe) {
                        log.error("Cannot save DrugPrice for:\nNDC = %s\nNPD ID = %s\nEffective Date = %d"
                                          .formatted(ndc, npdId, effDate), dpe);
                    }

                    return dp;
                }).forEach(dp -> {
                    if (dp != null) {
                        dp.setPostId(postId);

                        var savetimestart = System.currentTimeMillis();
                        HibernateUtil.save(dp, session);
                        var savetime = (System.currentTimeMillis() - savetimestart)/1000;

                        if (savetime > SAVE_WARN_THRESHOLD) {
                            log.warn("SAVE time exceeded %d seconds....................".formatted(SAVE_WARN_THRESHOLD));
                        }
                        if (++itemsSaved[0] % COMMIT_BOUNDARY == 0) {
                            var committimestart = System.currentTimeMillis();
                            session.getTransaction().commit();
                            var committime = (System.currentTimeMillis() - committimestart)/1000;
                            var curRuntime = ((System.currentTimeMillis() - startTime)/1000/60.0);

                            log.info("Committed %d [%d] items in %d seconds. Runtime: %.2f minutes."
                                             .formatted(COMMIT_BOUNDARY, itemsSaved[0], committime, curRuntime));

                            session.beginTransaction();
                        }
                    }
                });

                session.getTransaction().commit();

                log.info("%d items saved in %d minutes"
                                 .formatted(itemsSaved[0], ((System.currentTimeMillis() - startTime)/1000/60)));
            }
            catch (Exception e) {
                log.error("An error occurred while processing NADAC data. Rolling back transaction. Ran for %d minutes."
                                  .formatted(((System.currentTimeMillis() - startTime)/1000/60)), e);

                if (session.getTransaction().isActive()) {
                    session.getTransaction().rollback();
                }
            }
            finally {
                if (csvFile != null) {
                    csvFile.close();
                }

                try {
                    session.close();
                }
                catch (Exception se) {
                    log.error("Failed to close session.", se);
                }
            }
        }
        else {
            log.warn("Unable to find the 'nadac' wholesaler. Data will not be processed.");
        }
    }

}
