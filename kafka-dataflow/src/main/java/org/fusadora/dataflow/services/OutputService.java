package org.fusadora.dataflow.services;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public interface OutputService extends Serializable {
    WriteResult writeToBqFileLoad(PCollection<TableRow> input, String transformName, String bqTableName,
                                  TableSchema bqTableSchema, String partitionType);

    void writeToKafka(PCollection<KV<String, String>> input, String transformName, String brokerHost, String topicName);
}
