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
import org.apache.kafka.connect.header.Header;
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
  // Thought, we could also have:
//  private static final String PARTITION_FORMAT = "streamUuid=%s/entityId=%s/%s-%s/%s/%s";

  @Override
  public void configure(Map<String, Object> config) {
    log.info("Configuring EVAnalyticsOcpiPartitioner...");
    super.configure(config);
  }

  private String getStreamUuidFromHeaders(SinkRecord sinkRecord) {
    log.warn("Trying to find the offering_uuid in the headers instead...");
    String streamUuid = null;
    // This is a job for a filtering lambda
    for (Header header : sinkRecord.headers()) {
      System.out.println("header key => "
              + header.key()
              + "header value => "
              + header.value().toString());
      if (header.key().equals("offering_uuid")) {
        log.info("setting streamUuid to: " + header.value().toString());
        streamUuid = header.value().toString();
      }
    }
    return streamUuid == null ? "noStreamIdFound" : streamUuid;
  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    // Gets streamUuid from the key (or, indeed, the header) of the message
    // Decode value of sinkRecord as JSON using jackson
    // If it's not JSON, throw an exception?
    //    somehow exit gracefully
    // get entityId from the Json.value of 'id'
    // from the 'timestamp' Json.value, of form '2021-08-31T17:24:13Z' (zulu time)
    //    get YYYY-MM, create partition
    //      get DD, create partition
    //        get HH, create partition

    // NOTE that any Exceptions thrown from this class
    // will helpfully crash the entire connector. This means it will not
    // start up correctly. This means there is no data lake

    // for inspiration, see:
    // https://stackoverflow.com/questions/57499274/implementing-a-kafka-connect-custom-partitioner
    // for jackson, see:
    // https://www.tutorialspoint.com/jackson/jackson_quick_guide.htm
    log.info("encoding partition with EVAnalyticsOcpiPartitioner...");

    log.info("Parsing value...");

    String value = sinkRecord.value().toString();
    log.info("Value: " + value);
    String streamUuid = "";

    try {
      log.info("Assuming streamUuid is in the key...");
      streamUuid = sinkRecord.key().toString();
      UUID.fromString(streamUuid);
    } catch (IllegalArgumentException exception) {
      String msg = "Key is not a valid uuid, it therefore probably not a stream id";
      log.error(msg);
      throw new PartitionException(msg);
    } catch (NullPointerException exception) {
      // on the aws.db.data-streams.records.0 topic the offering_uuid is in the headers...
      String msg = "Record does not have a key";
      log.warn(msg);
      // This seems to work, hooray
      // TODO: add unit tests for streamUuid in header
      streamUuid = getStreamUuidFromHeaders(sinkRecord);
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

    // Note that relying on id and timestamp will not be enough to guarantee the
    // payload is an OCPI format if other payloads come in that are NOT OCPI on the same
    // topic. This is a naive approach!
    if (ocpiPayload.getId() == null || ocpiPayload.getTimestamp() == null) {
      log.warn("No ocpi mapping found, sending payload to non stream uuid partition...");
      String msg = "Could not map this payload to a known OCPI class";
      // Should probably map to a different partition e.g. not-ocpi
      // It's probably a good idea to see if we can still parse the timestamp, though
      // If not that, just use the timestamp of the message supplied by kakfa?
      return String.format("not-ocpi/%s", streamUuid);
    }

    log.info("Mapping record value into object...");
    log.info(ocpiPayload.toString());
    String entityId = ocpiPayload.getId();
    String timestamp = ocpiPayload.getTimestamp();

    try {
      // timestamp format is e.g. "2021-08-31T17:24:13Z"
      // There's definitely a better way to work with timestamps in Java
      // TODO: for non ocpi payloads that are still mapped, format may NOT be Zulu!
      // TODO: add unit tests for timestamp processing
      // Error: Could not parse YYYY-MM/DD/HH values from timestamp: 1655925271224
      // => Cannot build partition (org.apache.kafka.connect.runtime.WorkerSinkTask:612)
      // it might be as well to parse _anything_ in a timestamp field into
      // a Java Datetime in the UTC timezone and get the YYYY-MM/DD/HH that way
      // TODO: do we even need an HH partition? Would it make glue crawling more difficult?
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
