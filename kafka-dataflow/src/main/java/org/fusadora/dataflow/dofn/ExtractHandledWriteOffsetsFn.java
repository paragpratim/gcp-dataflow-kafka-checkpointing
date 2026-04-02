package org.fusadora.dataflow.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.fusadora.dataflow.common.KafkaMetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts topic-partition key and offset from BigQuery handled rows.
 */
@SuppressWarnings("unused") // Instantiated from pipeline transform wiring
public class ExtractHandledWriteOffsetsFn extends SimpleFunction<TableRow, KV<String, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractHandledWriteOffsetsFn.class);
    private static final String KEY_SEPARATOR = ":";
    private static final String INVALID_KEY = "";
    private static final long INVALID_OFFSET = Long.MIN_VALUE;

    @Override
    public KV<String, Long> apply(TableRow row) {
        if (row == null) {
            return KV.of(INVALID_KEY, INVALID_OFFSET);
        }

        Object topic = row.get(KafkaMetadataConstants.META_KAFKA_TOPIC);
        Object partition = row.get(KafkaMetadataConstants.META_KAFKA_PARTITION);
        Object offset = row.get(KafkaMetadataConstants.META_KAFKA_OFFSET);
        if (topic == null || partition == null || offset == null) {
            return KV.of(INVALID_KEY, INVALID_OFFSET);
        }

        try {
            int partitionInt = Integer.parseInt(partition.toString());
            long offsetLong = Long.parseLong(offset.toString());
            return KV.of(topic + KEY_SEPARATOR + partitionInt, offsetLong);
        } catch (NumberFormatException nfe) {
            LOG.warn("Unable to parse success metadata for checkpoint commit topic={} partition={} offset={}",
                    topic, partition, offset);
            return KV.of(INVALID_KEY, INVALID_OFFSET);
        }
    }
}


