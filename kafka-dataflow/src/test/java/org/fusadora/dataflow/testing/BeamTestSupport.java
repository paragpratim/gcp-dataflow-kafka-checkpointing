package org.fusadora.dataflow.testing;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.fusadora.dataflow.dto.KafkaEventEnvelope;
import org.fusadora.dataflow.dto.TopicConfig;
import org.fusadora.dataflow.ptransform.WriteRawMessageTransform;
import org.fusadora.dataflow.services.CheckpointService;
import org.fusadora.dataflow.services.InputService;
import org.fusadora.dataflow.services.OutputService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class BeamTestSupport {

    private BeamTestSupport() {
    }

    public static KafkaEventEnvelope envelope(String topic, int partition, long offset, String payload) {
        return new KafkaEventEnvelope(topic, partition, offset, payload);
    }

    public static KafkaRecord<String, String> kafkaRecord(String topic, int partition, long offset, String payload) {
        return new KafkaRecord<>(topic, partition, offset, offset,
                KafkaTimestampType.CREATE_TIME, new RecordHeaders(), KV.of("key-" + offset, payload));
    }

    public static TopicConfig topicConfig(String topicName, String datasetName) {
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(topicName);
        topicConfig.setDatasetName(datasetName);
        return topicConfig;
    }

    public static KafkaRecordCoder<String, String> kafkaRecordCoder() {
        return KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
    }

    public static final class RecordingCheckpointService implements CheckpointService {

        private static final Map<String, Long> NEXT_OFFSETS = new ConcurrentHashMap<>();
        private static final List<CheckpointUpdate> UPDATES = Collections.synchronizedList(new ArrayList<>());

        @SuppressWarnings("unused")
        public RecordingCheckpointService() {
        }

        public RecordingCheckpointService(Map<String, Long> initialNextOffsets) {
            reset(initialNextOffsets);
        }

        public static void reset() {
            NEXT_OFFSETS.clear();
            UPDATES.clear();
        }

        public static void reset(Map<String, Long> initialNextOffsets) {
            reset();
            NEXT_OFFSETS.putAll(initialNextOffsets);
        }

        public static long nextOffset(String topic, int partition) {
            return NEXT_OFFSETS.getOrDefault(docId(topic, partition), 0L);
        }

        public static List<CheckpointUpdate> updates() {
            synchronized (UPDATES) {
                return List.copyOf(UPDATES);
            }
        }

        @Override
        public long getNextOffsetToRead(String topic, int partition) {
            return nextOffset(topic, partition);
        }

        @Override
        public Map<Integer, Long> getTopicPartitionOffsets(String topic) {
            Map<Integer, Long> offsets = new ConcurrentHashMap<>();
            NEXT_OFFSETS.forEach((key, value) -> {
                String[] parts = key.split(":", -1);
                if (parts.length == 2 && parts[0].equals(topic)) {
                    offsets.put(Integer.parseInt(parts[1]), value);
                }
            });
            return offsets;
        }

        @Override
        public void updateOffsetCheckpoint(String topic, int partition, long lastAckedOffset, String jobId) {
            NEXT_OFFSETS.merge(docId(topic, partition), lastAckedOffset + 1, Math::max);
            UPDATES.add(new CheckpointUpdate(topic, partition, lastAckedOffset, jobId));
        }

        private static String docId(String topic, int partition) {
            return topic + ":" + partition;
        }
    }

    public record CheckpointUpdate(String topic, int partition, long lastAckedOffset, String jobId) {
    }

    @SuppressWarnings("unused")
    public static final class FakeInputService implements InputService {

        private final PTransform<PBegin, PCollection<KafkaRecord<String, String>>> sourceTransform;
        private final List<String> bootstrapTopics = new ArrayList<>();

        @SuppressWarnings("unused")
        public FakeInputService(PTransform<PBegin, PCollection<KafkaRecord<String, String>>> sourceTransform) {
            this.sourceTransform = sourceTransform;
        }

        @Override
        public void bootstrapOffsetsFromCheckpoint(String brokerIp, String topic) {
            bootstrapTopics.add(topic);
        }

        @Override
        public PCollection<KafkaRecord<String, String>> readFromKafka(Pipeline pipeline, String brokerIp, String topic,
                                                                      String transformName) {
            return pipeline.apply(transformName, sourceTransform);
        }

        @SuppressWarnings("unused")
        public List<String> getBootstrapTopics() {
            return List.copyOf(bootstrapTopics);
        }
    }

    @SuppressWarnings("unused")
    public static final class FakeOutputService implements OutputService {

        private final Set<Long> failingOffsets;
        private PCollection<TableRow> capturedRows;

        @SuppressWarnings("unused")
        public FakeOutputService(Set<Long> failingOffsets) {
            this.failingOffsets = failingOffsets;
        }

        @Override
        public WriteResult writeToBqFileLoad(PCollection<TableRow> input, String transformName, String bqTableName,
                                             TableSchema bqTableSchema, String partitionType) {
            this.capturedRows = input;

            PCollection<TableRow> successRows = input.apply(transformName + "-success-filter",
                    Filter.by(row -> row != null && !failingOffsets.contains(parseOffset(row))));

            PCollection<TableRow> failedRowValues = input.apply(transformName + "-failure-filter",
                    Filter.by(row -> row != null && failingOffsets.contains(parseOffset(row))));

            PCollection<BigQueryStorageApiInsertError> failedRows = failedRowValues.apply(
                    transformName + "-to-errors", ParDo.of(new DoFn<TableRow, BigQueryStorageApiInsertError>() {
                        @SuppressWarnings("unused")
                        @ProcessElement
                        public void processElement(ProcessContext context) {
                            context.output(new BigQueryStorageApiInsertError(context.element(), "forced test failure"));
                        }
                    }));
            return newWriteResult(input.getPipeline(), successRows, failedRows, transformName);
        }

        @SuppressWarnings("unused")
        public PCollection<TableRow> getCapturedRows() {
            return capturedRows;
        }
    }

    public static WriteResult newWriteResult(Pipeline pipeline,
                                             PCollection<TableRow> successfulStorageApiInserts,
                                             PCollection<BigQueryStorageApiInsertError> failedStorageApiInserts,
                                             String nameSuffix) {
        try {
            Method inMethod = WriteResult.class.getDeclaredMethod(
                    "in",
                    Pipeline.class,
                    TupleTag.class,
                    PCollection.class,
                    PCollection.class,
                    TupleTag.class,
                    PCollection.class,
                    TupleTag.class,
                    PCollection.class,
                    TupleTag.class,
                    PCollection.class);
            inMethod.setAccessible(true);

            PCollection<TableRow> emptyRows = pipeline.apply("EmptyRows-" + nameSuffix,
                    Create.empty(TableRowJsonCoder.of()));
            PCollection<TableDestination> emptyDestinations = pipeline.apply("EmptyDestinations-" + nameSuffix,
                    Create.empty(SerializableCoder.of(TableDestination.class)));

            return (WriteResult) inMethod.invoke(
                    null,
                    pipeline,
                    new TupleTag<TableRow>() {
                    },
                    emptyRows,
                    emptyRows,
                    new TupleTag<TableDestination>() {
                    },
                    emptyDestinations,
                    new TupleTag<BigQueryStorageApiInsertError>() {
                    },
                    failedStorageApiInserts,
                    new TupleTag<TableRow>() {
                    },
                    successfulStorageApiInserts);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException reflectionException) {
            throw new IllegalStateException("Failed to construct WriteResult for tests", reflectionException);
        }
    }

    public static long parseOffset(TableRow row) {
        return Long.parseLong(row.get(WriteRawMessageTransform.META_KAFKA_OFFSET).toString());
    }

    @SuppressWarnings("unused")
    public static Collection<Long> offsetsFromRows(Iterable<TableRow> rows) {
        List<Long> offsets = new ArrayList<>();
        for (TableRow row : rows) {
            offsets.add(parseOffset(row));
        }
        return offsets;
    }
}

