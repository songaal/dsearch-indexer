package com.danawa.fastcatx.indexer;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class IndexService {

    private static Logger logger = LoggerFactory.getLogger(IndexService.class);

    private int count;

    public int getCount() {
        return count;
    }

    public void index(Ingester ingester, String host, Integer port, String scheme, String index, Integer bulkSize) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));

        count = 0;
        long start = System.currentTimeMillis();
        BulkRequest request = new BulkRequest();
        long time = System.nanoTime();
        while (ingester.hasNext()) {
            count++;
            Map<String, Object> record = ingester.next();

            request.add(new IndexRequest(index).source(record, XContentType.JSON));

            if (count % bulkSize == 0) {
                BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                logger.debug("bulk! {}", count);
                request = new BulkRequest();
            }

            if (count % 100000 == 0) {
                logger.info("{} ROWS FLUSHED! in {}ms", count, (System.nanoTime() - time) / 1000000);
            }
        }

        if (count % bulkSize > 0) {
            //나머지..
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            checkResponse(bulkResponse);
            logger.debug("Final bulk! {}", count);
        }

        long totalTime = System.currentTimeMillis() - start;
        logger.info("Flush Finished! doc[{}] elapsed[{}m]", count, totalTime / 1000 / 60);
    }

    private void checkResponse(BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    logger.error("Doc index error >> {}", failure);
                }
            }
        }
    }
}
