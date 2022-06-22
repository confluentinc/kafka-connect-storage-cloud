/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.s3.extensions;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;


public class EVAnalyticsOcpiPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(EVAnalyticsOcpiPartitioner.class);
  // streamUuid/entityId/YYYY-MM/DD/HH
  private static final String PARTITION_FORMAT = "%s/%s/%s-%s/%s/%s";

  @Override
  public void configure(Map<String, Object> config) {
    log.info("Configuring EVAnalyticsOcpiPartitioner...");
    super.configure(config);
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    // Gets streamUuid from the key of the message
    // Decode value of sinkRecord as JSON using jackson
    // If it's not JSON, throw an exception?
    //    somehow exit gracefully
    // get entityId from the Json.value of 'id'
    // from the 'timestamp' Json.value, of form '2021-08-31T17:24:13Z' (zulu time)
    //    get YYYY-MM, create partition
    //      get DD, create partition
    //        get HH, create partition

    // for inspiration, see:
    // https://stackoverflow.com/questions/57499274/implementing-a-kafka-connect-custom-partitioner
    // for jackson, see:
    // https://www.tutorialspoint.com/jackson/jackson_quick_guide.htm
    log.info("encoding partition with EVAnalyticsOcpiPartitioner...");

    String value = sinkRecord.value().toString();
    String streamUuid = sinkRecord.key().toString();

    try {
      UUID.fromString(streamUuid);
    } catch (IllegalArgumentException exception) {
      String msg = "Key is not a valid uuid, it therefore probably not a stream id";
      log.error(msg);
      throw new PartitionException(msg);
    }

    ObjectMapper mapper = new ObjectMapper();

    // We don't want to have to extend our POJO with every single
    // field that appears in a payload, we only care about the fields
    // that make up the partitions in S3, hence this FAIL_ON_UNKNOWN_PROPERTIES
    // setting
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // the value of the timestamp can either be:
    // { timestamp: string } (in an OCPI session payload)
    // or
    // { timestamp: { type: 'Property', value: string } } (in an OCPI location payload)
    // We account for both with some sort of generic function...

    OcpiPayload ocpiPayload = null;

    // An alternative approach to this could be to use a custom JsonDeserializer:
    // https://www.baeldung.com/jackson-nested-values#mapping-with-custom-jsondeserializer
    try {
      // try to parse into sessions
      ocpiPayload = mapper.readValue(value, OcpiSessionsPayload.class);
      log.info("Mapped to OcpiSessionsPayload");
    } catch (JacksonException e) {
      log.warn("Could not parse payload into sessions POJO");
    }

    try {
      // try to parse into location
      ocpiPayload = mapper.readValue(value, OcpiLocationsPayload.class);
      log.info("Mapped to OcpiLocationsPayload");
    } catch (JacksonException e) {
      log.warn("Could not parse payload into location POJO");
    }

    if (ocpiPayload.getId() == null || ocpiPayload.getTimestamp() == null) {
      log.error("No ocpi mapping found, try again...");
      String msg = "Could not map this payload to a known OCPI class";
      throw new PartitionException(msg);
    }

    log.info("Mapping record value into object...");
    log.info(ocpiPayload.toString());
    String entityId = ocpiPayload.getId();
    String timestamp = ocpiPayload.getTimestamp();

    try {
      // timestamp format is e.g. "2021-08-31T17:24:13Z"
      // There's definitely a better way to work with timestamps in Java
      String[] splitTimestamp = timestamp.split("-");
      String year = splitTimestamp[0];
      String month = splitTimestamp[1];
      String[] dayTime = splitTimestamp[2].split("T");
      String day = dayTime[0];
      String hour = dayTime[1].substring(0, 2);
      return String.format(PARTITION_FORMAT, streamUuid, entityId, year, month, day, hour);
    } catch (Exception e) {
      String msg = "Could not parse YYYY-MM/DD/HH values from timestamp: "
              + timestamp
              + " => Cannot build partition";
      log.error(msg);
      throw new PartitionException(msg);
    }
  }

  @Override
  public List<T> partitionFields() {
    // TODO: what are partition fields?
    log.info("Returning partition fields from EVAnalyticsOcpiPartitioner...");
    return super.partitionFields();
  }
}
