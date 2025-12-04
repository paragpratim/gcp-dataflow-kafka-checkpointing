package org.fusadora.dataflow.services;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public interface InputService extends Serializable {

    PCollection<KafkaRecord<String, String>> readFromKafka(Pipeline pipeline, String brokerIp, String topic, String transformName);

}
