package io.confluent.connect.s3;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.CoerceToSegmentPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.hotstar.kafka.connect.transforms.ProtoToPayloadTransform;

public class S3SinkTaskV2 extends S3SinkTask {
    private ExecutorService executorService;
    public static final long THREAD_KEEP_ALIVE_TIME_SECONDS = 60L;
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SinkTaskV2.class);

    private static final ProtoToPayloadTransform.Value<SinkRecord> protoToPayloadTransform = new ProtoToPayloadTransform.Value<>();
    private static final CoerceToSegmentPayload<SinkRecord> coerceToSegmentPayloadTransform = new CoerceToSegmentPayload.Value<>();

    static {
        // This is needed to initialise the statsD client with default values.
        protoToPayloadTransform.configure(new HashMap<>());

    }


    @Override
    public void start(Map<String, String> props) {
        super.start(props);
        int transformThreadPoolMaxSize = Integer.parseInt(props.get("transform.threads.max"));
        this.executorService = new ThreadPoolExecutor(1, transformThreadPoolMaxSize,
                THREAD_KEEP_ALIVE_TIME_SECONDS,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        transformInParallel(records);
        super.put(records);
    }

    private Collection<SinkRecord> transformInParallel(Collection<SinkRecord> sinkRecords) {
        int batchSize = sinkRecords.size();
        SinkRecord[] kafkaRecordArray = sinkRecords.toArray(new SinkRecord[0]);
        List<Future<SinkRecord>> futureList = new ArrayList<>(batchSize);
//        LOGGER.debug("In transformInParallel Method.");
        for (int i = 0; i < batchSize; i++) {
            int recordNumber = i;
            futureList.add(executorService.submit((Callable) () -> transformProtoToEventPayload(kafkaRecordArray[recordNumber])));
        }
        List<SinkRecord> transformedRecords = new ArrayList<>(batchSize);
        futureList.stream().forEach(future -> {
            try {
                SinkRecord transformedRecord = future.get();
                transformedRecords.add(transformedRecord);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        return transformedRecords;
    }

    private SinkRecord transformProtoToEventPayload(SinkRecord sinkRecord){
//        LOGGER.warn("Staring protoToPayloadTransform  on {}",sinkRecord);
        SinkRecord intermediateRecord = protoToPayloadTransform.apply(sinkRecord);
//        LOGGER.warn("Staring coerceToSegmentPayload  on {}",intermediateRecord);
        return coerceToSegmentPayloadTransform.apply(intermediateRecord);
    }
}
