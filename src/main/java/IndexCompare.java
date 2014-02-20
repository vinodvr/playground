import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * Created with IntelliJ IDEA.
 * User: vinodvr
 * Date: 20/2/14
 * Time: 5:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class IndexCompare {

    private static TransportClient client;
    static String clusterName = "vinodes1";
    static String hostname = "localhost";

    private static void log(Object o) {
        System.out.println(o.toString());
    }

    public static void main(String[] args) throws Exception {
        hostname = args[0];
        clusterName = args[1];
        final String idx1 = args[2];
        final String idx2 = args[3];

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName) // custer name
                .put("client.transport.sniff", true) // When a new node is added sniff adds it to the cluster
                .put("client.transport.nodes_sampler_interval","60s")
                .put("client.transport.ping_timeout","60s")
                .build();
        log("Creating elastic search client, hostName: " + hostname + ", clusterName: " + clusterName);

        client = new TransportClient(settings);
        client = client.addTransportAddress(new InetSocketTransportAddress(hostname, 9300));

        Thread t1 = getThread(idx1);
        t1.start();
        log("started thread-1 for idx : " + idx1);

        Thread.sleep(1L);

        Thread t2 = getThread(idx2);
        t2.start();
        log("started thread-2 for idx : " + idx2);

        t1.join();
        t2.join();

        log("DONE!!");

    }

    private static Thread getThread(final String index) {
        final SearchResponse scrollResponse1 = client.prepareSearch(index)
                .setSearchType(SearchType.SCAN)
                .setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(1000)
                .execute()
                .actionGet();
        return new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    SearchResponse scrollResp = scrollResponse1;
                    int i = 0;
                    File file = new File(index + "_" + System.currentTimeMillis());
                    if (!file.exists()) {
                        file.createNewFile();
                        log("Path : " + file.getAbsolutePath());
                    }
                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);
                    while (true) {
                        scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                        for (SearchHit hit : scrollResp.getHits()) {
                            String sourceAsString = hit.getSourceAsString();
                            String id = hit.getId();
                            String toWrite = id + "\t" +  sourceAsString;
                            bw.write(toWrite);
                            bw.newLine();
                            if (++i % 10000 == 0) {
                                log(Thread.currentThread().getName() + " Completed i = " + i);
                            }
                        }
                        //Break condition: No hits are returned
                        if (scrollResp.getHits().getHits().length == 0) {
                            log("completed thread-1 for idx : " + index);
                            bw.close();
                            fw.close();
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
