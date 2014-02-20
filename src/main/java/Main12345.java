import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: vinodvr
 * Date: 20/1/14
 * Time: 9:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class Main12345 {
    private static  TransportClient client;

    public static void main(String[] args) throws Exception {
        try {
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", "ch-hoodoo-search") // custer name
                    .put("client.transport.sniff", true) // When a new node is added sniff adds it to the cluster
                    .put("client.transport.nodes_sampler_interval","60s")
                    .put("client.transport.ping_timeout","60s")
                    .build();
            log("Creating elastic search client");
            client = new TransportClient(settings);
            client = client.addTransportAddress(new InetSocketTransportAddress("sp-cms-audit-search1.ch.flipkart.com", 9300));
            client = client.addTransportAddress(new InetSocketTransportAddress("sp-cms-audit-search2.ch.flipkart.com", 9300));
            client = client.addTransportAddress(new InetSocketTransportAddress("sp-cms-audit-search3.ch.flipkart.com", 9300));
            doIt();
        } finally {
            es.shutdownNow();
            client.close();
        }
    }


    static ExecutorService es = Executors.newFixedThreadPool(10);
    private static void doIt() throws InterruptedException, ExecutionException {
        for (int i = 0; i < 20; i++) {
            List<Future<Void>> futures = new ArrayList<Future<Void>>();
            for (int j = 0; j < 100; j++) {
                final int finalI = i;
                final int finalJ = j;
                Future<Void> future = es.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        long stTime = System.currentTimeMillis();
                        QueryBuilder builder = new BoolQueryBuilder()
                                .must(new MatchQueryBuilder("vertical", "mobile")
                                        .type(MatchQueryBuilder.Type.BOOLEAN)
                                        .operator(MatchQueryBuilder.Operator.AND));
                        if (finalI == 0 && finalJ == 0) {
                            log(builder);
                        }
                        SearchResponse response = client.prepareSearch("fk_sp_audit_listing_audit_0")
                                .setTypes("fk_sp_audit_listing_audit_0")
                                .setQuery(builder)
                                .setSize(10)
                                .execute()
                                .actionGet();
                        if (response.getHits().hits().length != 10) {
                            throw new IllegalStateException("incorrect hits!!");
                        }
                        long taken = System.currentTimeMillis() - stTime;
                        if (taken > 10000) {
                            throw new IllegalStateException("took more than 10 sec : " + taken + " ms");
                        }
                        return null;  //To change body of implemented methods use File | Settings | File Templates.
                    }
                });
                futures.add(future);
            }

            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof NoNodeAvailableException) {
                        log("NoNodeAvailableException happened!!");
                    } else {
                        throw e;
                    }
                }
            }

            Double millis = Math.pow(2, i + 11) * 1000;
            log("Completed loop for i = " + i + " now sleeping for : " + millis/1000 + " secs at : " + new Date());
            Thread.sleep(millis.longValue());
        }
    }

    private static void log(Object o) {
        System.out.println(o.toString());
    }
}
