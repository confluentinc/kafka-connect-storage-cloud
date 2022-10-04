/**
 * These few classes needed for Integration tests were copied in from:
 *   https://github.com/apache/parquet-mr/tree/apache-parquet-1.11.2/parquet-tools
 * That way, we can avoid dependency hell due to parquet-tools-1.11.x bundling an
 * old/incompatible org.apache.parquet.schema package
 * N.B. parquet-tools is deprecated/removed in parquet-1.12.x
 */
package io.confluent.connect.s3.integration.parquet.tools;
