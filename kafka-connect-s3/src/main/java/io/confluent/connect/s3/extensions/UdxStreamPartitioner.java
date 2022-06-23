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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

import static java.lang.Integer.parseInt;


public class UdxStreamPartitioner<T> extends DefaultPartitioner<T> {
  private static final Logger log = LoggerFactory.getLogger(UdxStreamPartitioner.class);
  private static final String UDX_PARTITION_FORMAT = "streamUuid=%s/entityId=%s/%d-%02d/day=%02d/hour=%02d";

  @Override
  public void configure(Map<String, Object> config) {
    log.info("Configuring UdxStreamPartitioner...");
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

  private boolean timestampCanParseToLong(String timestamp) {
    try {
      Long.parseLong(timestamp);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private DateTime parseTimestampFromPayload(String timestamp) {
    log.info("parsing timestamp : " + timestamp);
    // Here the timestamp could be a long as a string or an ISO timestamp
    // We must therefore see if a long can be parsed from the string...
    if (timestampCanParseToLong(timestamp)) {
      long timestampAsLong = Long.parseLong(timestamp);
      log.info("Parsed timestamp as : " + timestampAsLong);
      return new DateTime(timestampAsLong).withZone(DateTimeZone.UTC);
    }

    try {
      return new DateTime(timestamp).withZone(DateTimeZone.UTC);
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new PartitionException("Could not parse timestamp from payload");
    }

  }

  private String generateCompletePartition(String streamUuid, String entityId, DateTime timestamp) {
    int year = timestamp.getYear();
    int month = timestamp.getMonthOfYear();
    int day = timestamp.getDayOfMonth();
    int hour = timestamp.getHourOfDay();
    return String.format(UDX_PARTITION_FORMAT, streamUuid, entityId, year, month, day, hour);
  }

  private String generateInvalidPayloadPartition(String streamUuid) {
    return String.format("invalidIdOrTimestamp/%s", streamUuid);
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
    log.info("encoding partition with UdxStreamPartitioner...");

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

    UdxPayload udxPayload = null;

    // An alternative approach to this could be to use a custom JsonDeserializer:
    // https://www.baeldung.com/jackson-nested-values#mapping-with-custom-jsondeserializer
    // The use of concrete *Payload classes does make things very explicit, though
    try {
      // try to parse into sessions
      udxPayload = mapper.readValue(value, FlatTimestampPayload.class);
      log.info("Mapped to FlatTimestampPayload");
    } catch (JacksonException e) {
      log.warn("Could not parse payload into sessions POJO");
    }

    try {
      // try to parse into location
      udxPayload = mapper.readValue(value, NestedTimestampPayload.class);
      log.info("Mapped to NestedTimestampPayload");
    } catch (JacksonException e) {
      log.warn("Could not parse payload into location POJO");
    }

    // Note that relying on id and timestamp will not be enough to guarantee the
    // payload is an OCPI format if other payloads come in that are NOT OCPI on the same
    // topic. This is a naive approach!
    if (udxPayload.getId() == null || udxPayload.getTimestamp() == null) {
      log.warn("No UdxPayload mapping found, sending payload to non stream uuid partition...");
      String msg = "Could not map this payload to a defined UdxPayload class";
      // It's probably a good idea to see if we can still parse the timestamp, though
      // If not that, just use the timestamp of the message supplied by kakfa?
      return generateInvalidPayloadPartition(streamUuid);
    }

    log.info("Mapping record value into object...");
    log.info(udxPayload.toString());
    String entityId = udxPayload.getId();
    String timestamp = udxPayload.getTimestamp();

    try {
      DateTime parsedTimestamp = parseTimestampFromPayload(timestamp);
      return generateCompletePartition(streamUuid, entityId, parsedTimestamp);
    } catch (Exception e) {
      // If we can't parse the timestamp, should we
      log.error(e.getMessage());
      String msg = "Could not parse YYYY-MM/DD/HH values from timestamp: "
              + timestamp
              + " => Cannot build partition";
      log.error(msg);
      throw new PartitionException(msg);
    }
  }
}
