import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: vinodvr
 * Date: 20/1/14
 * Time: 9:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class ESSearchBenchmark {
    private static  TransportClient client;

    public static void main(String[] args) throws Exception {
        try {
            hostname = args[0];
            clusterName = args[1];

            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", clusterName) // custer name
                    .put("client.transport.sniff", true) // When a new node is added sniff adds it to the cluster
                    .put("client.transport.nodes_sampler_interval","60s")
                    .put("client.transport.ping_timeout","60s")
                    .build();
            log("Creating elastic search client, hostName: " + hostname + ", clusterName: " + clusterName);

            client = new TransportClient(settings);
            client = client.addTransportAddress(new InetSocketTransportAddress(hostname, 9300));


            String opType = args[2];
            if (opType.equals("1")) {
                populateData();
            } else if (opType.equals("2")) {
                searchByTermQuery();
            } else if (opType.equals("3")) {
                searchByTermFilter();
            } else if (opType.equals("4")) {
                searchByMatchQuery();
            }


        } finally {
            client.close();
        }
    }

    // curl -XPOST http://localhost:9200/rels -d @schema.json
    /*{
        "mappings": {
        "mail": {
            "fromId": {"type" : "string", "store": "yes", "index": "not_analyzed"},
            "toId":   {"type" : "string", "store": "yes", "index": "not_analyzed"}
        }
    }
    }*/


    static String clusterName = "vinodes1";
    static String hostname = "localhost";
    static int nThreads = 20;
    static Random r = new Random(System.currentTimeMillis());
    static final MetricRegistry metrics = new MetricRegistry();
    static final Timer timer = metrics.timer("opTimer");
    static final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    static {
        reporter.start(1, TimeUnit.MINUTES);
    }


    private static void searchByTermFilter() throws InterruptedException, ExecutionException {
        ExecutorService es = getExecutorService();
        final AtomicInteger progress = new AtomicInteger(0);
        final AtomicInteger errCnt = new AtomicInteger(0);
        final int totalCnt = 500000;
        for (int i = 0; i < totalCnt; i++) {
            final int finalI = i;
            es.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        int fromId = r.nextInt(500000);
                        final Timer.Context context = timer.time();
                        try {
                            ConstantScoreQueryBuilder scoreBuilder = QueryBuilders.constantScoreQuery(
                                    FilterBuilders.termFilter("fromId", String.valueOf(fromId)));
                            SearchRequestBuilder sourceBuilder = client.prepareSearch("rels")
                                    .setTypes("mail")
                                    .setQuery(scoreBuilder)
                                    .setSize(100);
                            SearchResponse searchResponse = sourceBuilder.execute().get();

                            /*SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                                    .filter(new TermFilterBuilder("fromId", String.valueOf(fromId)))
                                    .size(100);

                            SearchResponse searchResponse = client.search(
                                    new SearchRequest("rels").types("mail")
                                    .source(sourceBuilder)).actionGet();*/
                            if (finalI == 0) {
                                log(sourceBuilder.toString());
                            }
                            SearchHits hits = searchResponse.getHits();

                            if (hits.hits().length <= 0) {
                                throw new IllegalStateException("No results found");
                            }
                            int i1 = progress.incrementAndGet();
                            if (i1 % 1000 == 0) {
                                log("Searched : " + i1);
                            }
                        } finally {
                            context.stop();
                        }
                    } catch (Exception e) {
                        errCnt.incrementAndGet();
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
        log("ErrCnt : " + errCnt.get() + ", ProgressCnt : " + progress.get());
        reporter.report();
    }

    private static ExecutorService getExecutorService() {
        return Executors.newFixedThreadPool(nThreads);
    }


    private static void searchByMatchQuery() throws InterruptedException, ExecutionException {
        ExecutorService es = getExecutorService();
        final AtomicInteger progress = new AtomicInteger(0);
        final AtomicInteger errCnt = new AtomicInteger(0);
        final int totalCnt = 500000;
        for (int i = 0; i < totalCnt; i++) {
            final int finalI = i;
            es.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        int fromId = r.nextInt(totalCnt);
                        final Timer.Context context = timer.time();
                        try {
                            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                                    .query(new MatchQueryBuilder("fromId", String.valueOf(fromId)));
                            if (finalI == 0) {
                                log(sourceBuilder.toString());
                            }
                            SearchResponse searchResponse = client.search(new SearchRequest("rels").types("mail")
                                    .source(sourceBuilder.size(100))).actionGet();
                            SearchHits hits = searchResponse.getHits();

                            if (hits.hits().length <= 0) {
                                throw new IllegalStateException("No results found");
                            }
                            int i1 = progress.incrementAndGet();
                            if (i1 % 1000 == 0) {
                                log("Searched : " + i1);
                            }
                        } finally {
                            context.stop();
                        }
                    } catch (Exception e) {
                        errCnt.incrementAndGet();
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
        log("ErrCnt : " + errCnt.get() + ", ProgressCnt : " + progress.get());
        reporter.report();
    }


    private static void searchByTermQuery() throws InterruptedException, ExecutionException {
        ExecutorService es = getExecutorService();
        final AtomicInteger progress = new AtomicInteger(0);
        final AtomicInteger errCnt = new AtomicInteger(0);
        final int totalCnt = 500000;
        for (int i = 0; i < totalCnt; i++) {
            final int finalI = i;
            es.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        int fromId = r.nextInt(totalCnt);
                        final Timer.Context context = timer.time();
                        try {
                            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                                    .query(new TermQueryBuilder("fromId", String.valueOf(fromId)));
                            if (finalI == 0) {
                                log(sourceBuilder.toString());
                            }
                            SearchResponse searchResponse = client.search(new SearchRequest("rels").types("mail")
                                    .source(sourceBuilder.size(100))).actionGet();
                            SearchHits hits = searchResponse.getHits();
                            if (searchResponse.isTimedOut()) {
                                throw new IllegalStateException("Timed out");
                            }
                            if (hits.hits().length <= 0) {
                                throw new IllegalStateException("No results found");
                            }
                            int i1 = progress.incrementAndGet();
                            if (i1 % 1000 == 0) {
                                log("Searched : " + i1);
                            }
                        } finally {
                            context.stop();
                        }
                    } catch (Exception e) {
                        errCnt.incrementAndGet();
                        log(e.getMessage());
                    }
                    return null;
                }
            });
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
        log("ErrCnt : " + errCnt.get() + ", ProgressCnt : " + progress.get());
        reporter.report();
    }

    private static void populateData() throws InterruptedException, ExecutionException {
        ExecutorService es = getExecutorService();
        final int totalCnt = 500000;
        final AtomicInteger progress = new AtomicInteger(0);
        final AtomicInteger errCnt = new AtomicInteger(0);
        for (int i = 0; i < totalCnt; i++) {
            final int finalI = i;
            es.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        final Timer.Context context = timer.time();
                        try {
                            Map<String, Object> toPut = new HashMap<String, Object>();
                            toPut.put("fromId", String.valueOf(finalI));
                            List<String> toId = new ArrayList<String>();
                            for (int j = 0; j < 5; j++) {
                                toId.add(String.valueOf(r.nextInt(totalCnt)));
                            }
                            toPut.put("toId", toId);
                            client.index(new IndexRequest("rels", "mail").source(toPut)).get();
                            int i1 = progress.incrementAndGet();
                            if (i1 % 1000 == 0) {
                                log("Added : " + i1);
                            }
                        } finally {
                            context.stop();
                        }
                    } catch (Exception e) {
                        errCnt.incrementAndGet();
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
        log("ErrCnt : " + errCnt.get() + ", ProgressCnt : " + progress.get());
        reporter.report();
    }

    private static void log(Object o) {
        System.out.println(o.toString());
    }
}
