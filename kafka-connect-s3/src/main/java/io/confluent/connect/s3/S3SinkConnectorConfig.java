/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import io.confluent.connect.s3.format.bytearray.ByteArrayFormat;
import io.confluent.connect.s3.format.parquet.ParquetFormat;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.Locale;

import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.format.json.JsonFormat;
import io.confluent.connect.s3.storage.CompressionType;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.StorageSinkConnectorConfig;
import io.confluent.connect.storage.common.ComposableConfig;
import io.confluent.connect.storage.common.GenericRecommender;
import io.confluent.connect.storage.common.ParentValueRecommender;
import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.partitioner.DailyPartitioner;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.FieldPartitioner;
import io.confluent.connect.storage.partitioner.HourlyPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import io.confluent.connect.storage.partitioner.TimeBasedPartitioner;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class S3SinkConnectorConfig extends StorageSinkConnectorConfig {

  // S3 Group
  public static final String S3_BUCKET_CONFIG = "s3.bucket.name";

  public static final String SSEA_CONFIG = "s3.ssea.name";
  public static final String SSEA_DEFAULT = "";

  public static final String SSE_CUSTOMER_KEY = "s3.sse.customer.key";
  public static final Password SSE_CUSTOMER_KEY_DEFAULT = new Password(null);

  public static final String SSE_KMS_KEY_ID_CONFIG = "s3.sse.kms.key.id";
  public static final String SSE_KMS_KEY_ID_DEFAULT = "";

  public static final String PART_SIZE_CONFIG = "s3.part.size";
  public static final int PART_SIZE_DEFAULT = 25 * 1024 * 1024;

  public static final String WAN_MODE_CONFIG = "s3.wan.mode";
  private static final boolean WAN_MODE_DEFAULT = false;

  public static final String CREDENTIALS_PROVIDER_CLASS_CONFIG = "s3.credentials.provider.class";
  public static final Class<? extends AWSCredentialsProvider> CREDENTIALS_PROVIDER_CLASS_DEFAULT =
      DefaultAWSCredentialsProviderChain.class;
  /**
   * The properties that begin with this prefix will be used to configure a class, specified by
   * {@code s3.credentials.provider.class} if it implements {@link Configurable}.
   */
  public static final String CREDENTIALS_PROVIDER_CONFIG_PREFIX =
      CREDENTIALS_PROVIDER_CLASS_CONFIG.substring(
          0,
          CREDENTIALS_PROVIDER_CLASS_CONFIG.lastIndexOf(".") + 1
      );

  public static final String REGION_CONFIG = "s3.region";
  public static final String REGION_DEFAULT = Regions.DEFAULT_REGION.getName();

  public static final String ACL_CANNED_CONFIG = "s3.acl.canned";
  public static final String ACL_CANNED_DEFAULT = null;

  public static final String COMPRESSION_TYPE_CONFIG = "s3.compression.type";
  public static final String COMPRESSION_TYPE_DEFAULT = "none";

  public static final String S3_PART_RETRIES_CONFIG = "s3.part.retries";
  public static final int S3_PART_RETRIES_DEFAULT = 3;

  public static final String FORMAT_BYTEARRAY_EXTENSION_CONFIG = "format.bytearray.extension";
  public static final String FORMAT_BYTEARRAY_EXTENSION_DEFAULT = ".bin";

  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG = "format.bytearray.separator";
  public static final String FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT = System.lineSeparator();

  public static final String S3_PROXY_URL_CONFIG = "s3.proxy.url";
  public static final String S3_PROXY_URL_DEFAULT = "";

  public static final String S3_PROXY_USER_CONFIG = "s3.proxy.user";
  public static final String S3_PROXY_USER_DEFAULT = null;

  public static final String S3_PROXY_PASS_CONFIG = "s3.proxy.password";
  public static final Password S3_PROXY_PASS_DEFAULT = new Password(null);

  public static final String HEADERS_USE_EXPECT_CONTINUE_CONFIG =
      "s3.http.send.expect.continue";
  public static final boolean HEADERS_USE_EXPECT_CONTINUE_DEFAULT =
      ClientConfiguration.DEFAULT_USE_EXPECT_CONTINUE;

  public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
  public static final String BEHAVIOR_ON_NULL_VALUES_DEFAULT = BehaviorOnNullValues.FAIL.toString();

  /**
   * Maximum back-off time when retrying failed requests.
   */
  public static final int S3_RETRY_MAX_BACKOFF_TIME_MS = (int) TimeUnit.HOURS.toMillis(24);

  public static final String S3_RETRY_BACKOFF_CONFIG = "s3.retry.backoff.ms";
  public static final int S3_RETRY_BACKOFF_DEFAULT = 200;

  private final String name;

  private final StorageCommonConfig commonConfig;
  private final PartitionerConfig partitionerConfig;

  private final Map<String, ComposableConfig> propertyToConfig = new HashMap<>();
  private final Set<AbstractConfig> allConfigs = new HashSet<>();

  private static final GenericRecommender STORAGE_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender FORMAT_CLASS_RECOMMENDER = new GenericRecommender();
  private static final GenericRecommender PARTITIONER_CLASS_RECOMMENDER = new GenericRecommender();
  private static final ParentValueRecommender AVRO_COMPRESSION_RECOMMENDER
      = new ParentValueRecommender(FORMAT_CLASS_CONFIG, AvroFormat.class, AVRO_SUPPORTED_CODECS);

  static {
    STORAGE_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(S3Storage.class)
    );

    FORMAT_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            AvroFormat.class,
            JsonFormat.class,
            ByteArrayFormat.class,
            ParquetFormat.class
        )
    );

    PARTITIONER_CLASS_RECOMMENDER.addValidValues(
        Arrays.<Object>asList(
            DefaultPartitioner.class,
            HourlyPartitioner.class,
            DailyPartitioner.class,
            TimeBasedPartitioner.class,
            FieldPartitioner.class
        )
    );
  }

  public static ConfigDef newConfigDef() {
    ConfigDef configDef = StorageSinkConnectorConfig.newConfigDef(
        FORMAT_CLASS_RECOMMENDER,
        AVRO_COMPRESSION_RECOMMENDER
    );
    {
      final String group = "S3";
      int orderInGroup = 0;

      configDef.define(
          S3_BUCKET_CONFIG,
          Type.STRING,
          Importance.HIGH,
          "The S3 Bucket.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Bucket"
      );

      configDef.define(
          REGION_CONFIG,
          Type.STRING,
          REGION_DEFAULT,
          new RegionValidator(),
          Importance.MEDIUM,
          "The AWS region to be used the connector.",
          group,
          ++orderInGroup,
          Width.LONG,
          "AWS region",
          new RegionRecommender()
      );

      configDef.define(
          PART_SIZE_CONFIG,
          Type.INT,
          PART_SIZE_DEFAULT,
          new PartRange(),
          Importance.HIGH,
          "The Part Size in S3 Multi-part Uploads.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Part Size"
      );

      configDef.define(
          CREDENTIALS_PROVIDER_CLASS_CONFIG,
          Type.CLASS,
          CREDENTIALS_PROVIDER_CLASS_DEFAULT,
          new CredentialsProviderValidator(),
          Importance.LOW,
          "Credentials provider or provider chain to use for authentication to AWS. By default "
              + "the connector uses 'DefaultAWSCredentialsProviderChain'.",
          group,
          ++orderInGroup,
          Width.LONG,
          "AWS Credentials Provider Class"
      );

      List<String> validSsea = new ArrayList<>(SSEAlgorithm.values().length + 1);
      validSsea.add("");
      for (SSEAlgorithm algo : SSEAlgorithm.values()) {
        validSsea.add(algo.toString());
      }
      configDef.define(
          SSEA_CONFIG,
          Type.STRING,
          SSEA_DEFAULT,
          ConfigDef.ValidString.in(validSsea.toArray(new String[validSsea.size()])),
          Importance.LOW,
          "The S3 Server Side Encryption Algorithm.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Server Side Encryption Algorithm",
          new SseAlgorithmRecommender()
      );

      configDef.define(
          SSE_CUSTOMER_KEY,
          Type.PASSWORD,
          SSE_CUSTOMER_KEY_DEFAULT,
          Importance.LOW,
          "The S3 Server Side Encryption Customer-Provided Key (SSE-C).",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Server Side Encryption Customer-Provided Key (SSE-C)"
      );

      configDef.define(
          SSE_KMS_KEY_ID_CONFIG,
          Type.STRING,
          SSE_KMS_KEY_ID_DEFAULT,
          Importance.LOW,
          "The name of the AWS Key Management Service (AWS-KMS) key to be used for server side "
              + "encryption of the S3 objects. No encryption is used when no key is provided, but"
              + " it is enabled when '" + SSEAlgorithm.KMS + "' is specified as encryption "
              + "algorithm with a valid key name.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Server Side Encryption Key",
          new SseKmsKeyIdRecommender()
      );

      configDef.define(
          ACL_CANNED_CONFIG,
          Type.STRING,
          ACL_CANNED_DEFAULT,
          new CannedAclValidator(),
          Importance.LOW,
          "An S3 canned ACL header value to apply when writing objects.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Canned ACL"
      );

      configDef.define(
          WAN_MODE_CONFIG,
          Type.BOOLEAN,
          WAN_MODE_DEFAULT,
          Importance.MEDIUM,
          "Use S3 accelerated endpoint.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 accelerated endpoint enabled"
      );

      configDef.define(
          COMPRESSION_TYPE_CONFIG,
          Type.STRING,
          COMPRESSION_TYPE_DEFAULT,
          new CompressionTypeValidator(),
          Importance.LOW,
          "Compression type for file written to S3. "
          + "Applied when using JsonFormat or ByteArrayFormat. "
          + "Available values: none, gzip.",
          group,
          ++orderInGroup,
          Width.LONG,
          "Compression type"
      );

      configDef.define(
          S3_PART_RETRIES_CONFIG,
          Type.INT,
          S3_PART_RETRIES_DEFAULT,
          atLeast(0),
          Importance.MEDIUM,
          "Maximum number of retry attempts for failed requests. Zero means no retries. "
              + "The actual number of attempts is determined by the S3 client based on multiple "
              + "factors, including, but not limited to - "
              + "the value of this parameter, type of exception occurred, "
              + "throttling settings of the underlying S3 client, etc.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Part Upload Retries"
      );

      configDef.define(
          S3_RETRY_BACKOFF_CONFIG,
          Type.LONG,
          S3_RETRY_BACKOFF_DEFAULT,
          atLeast(0L),
          Importance.LOW,
          "How long to wait in milliseconds before attempting the first retry "
              + "of a failed S3 request. Upon a failure, this connector may wait up to twice as "
              + "long as the previous wait, up to the maximum number of retries. "
              + "This avoids retrying in a tight loop under failure scenarios.",
          group,
          ++orderInGroup,
          Width.SHORT,
          "Retry Backoff (ms)"
      );

      configDef.define(
          FORMAT_BYTEARRAY_EXTENSION_CONFIG,
          Type.STRING,
          FORMAT_BYTEARRAY_EXTENSION_DEFAULT,
          Importance.LOW,
          String.format(
              "Output file extension for ByteArrayFormat. Defaults to '%s'",
              FORMAT_BYTEARRAY_EXTENSION_DEFAULT
          ),
          group,
          ++orderInGroup,
          Width.LONG,
          "Output file extension for ByteArrayFormat"
      );

      configDef.define(
          FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG,
          Type.STRING,
          // Because ConfigKey automatically trims strings, we cannot set
          // the default here and instead inject null;
          // the default is applied in getFormatByteArrayLineSeparator().
          null,
          Importance.LOW,
          "String inserted between records for ByteArrayFormat. "
              + "Defaults to 'System.lineSeparator()' "
              + "and may contain escape sequences like '\\n'. "
              + "An input record that contains the line separator will look like "
              + "multiple records in the output S3 object.",
          group,
          ++orderInGroup,
          Width.LONG,
          "Line separator ByteArrayFormat"
      );

      configDef.define(
          S3_PROXY_URL_CONFIG,
          Type.STRING,
          S3_PROXY_URL_DEFAULT,
          Importance.LOW,
          "S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you"
              + " need to access S3 through a proxy.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Proxy Settings"
      );

      configDef.define(
          S3_PROXY_USER_CONFIG,
          Type.STRING,
          S3_PROXY_USER_DEFAULT,
          Importance.LOW,
          "S3 Proxy User. This property is meant to be used only if you"
              + " need to access S3 through a proxy. Using ``"
              + S3_PROXY_USER_CONFIG
              + "`` instead of embedding the username and password in ``"
              + S3_PROXY_URL_CONFIG
              + "`` allows the password to be hidden in the logs.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Proxy User"
      );

      configDef.define(
          S3_PROXY_PASS_CONFIG,
          Type.PASSWORD,
          S3_PROXY_PASS_DEFAULT,
          Importance.LOW,
          "S3 Proxy Password. This property is meant to be used only if you"
              + " need to access S3 through a proxy. Using ``"
              + S3_PROXY_PASS_CONFIG
              + "`` instead of embedding the username and password in ``"
              + S3_PROXY_URL_CONFIG
              + "`` allows the password to be hidden in the logs.",
          group,
          ++orderInGroup,
          Width.LONG,
          "S3 Proxy Password"
      );

      configDef.define(
          HEADERS_USE_EXPECT_CONTINUE_CONFIG,
          Type.BOOLEAN,
          HEADERS_USE_EXPECT_CONTINUE_DEFAULT,
          Importance.LOW,
          "Enable/disable use of the HTTP/1.1 handshake using EXPECT: 100-CONTINUE during "
              + "multi-part upload. If true, the client will wait for a 100 (CONTINUE) response "
              + "before sending the request body. Else, the client uploads the entire request "
              + "body without checking if the server is willing to accept the request.",
          group,
          ++orderInGroup,
          Width.SHORT,
          "S3 HTTP Send Uses Expect Continue"
      );

      configDef.define(
          BEHAVIOR_ON_NULL_VALUES_CONFIG,
          Type.STRING,
          BEHAVIOR_ON_NULL_VALUES_DEFAULT,
          BehaviorOnNullValues.VALIDATOR,
          Importance.LOW,
          "How to handle records with a null value (i.e. Kafka tombstone records)."
              + " Valid options are 'ignore' and 'fail'.",
          group,
          ++orderInGroup,
          Width.SHORT,
          "Behavior for null-valued records"
      );

    }
    return configDef;
  }

  public S3SinkConnectorConfig(Map<String, String> props) {
    this(newConfigDef(), props);
  }

  protected S3SinkConnectorConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
    ConfigDef storageCommonConfigDef = StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER);
    commonConfig = new StorageCommonConfig(storageCommonConfigDef, originalsStrings());
    ConfigDef partitionerConfigDef = PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER);
    partitionerConfig = new PartitionerConfig(partitionerConfigDef, originalsStrings());

    this.name = parseName(originalsStrings());
    addToGlobal(partitionerConfig);
    addToGlobal(commonConfig);
    addToGlobal(this);
  }

  private void addToGlobal(AbstractConfig config) {
    allConfigs.add(config);
    addConfig(config.values(), (ComposableConfig) config);
  }

  private void addConfig(Map<String, ?> parsedProps, ComposableConfig config) {
    for (String key : parsedProps.keySet()) {
      propertyToConfig.put(key, config);
    }
  }

  public String getBucketName() {
    return getString(S3_BUCKET_CONFIG);
  }

  public String getSsea() {
    return getString(SSEA_CONFIG);
  }

  public String getSseCustomerKey() {
    return getPassword(SSE_CUSTOMER_KEY).value();
  }

  public String getSseKmsKeyId() {
    return getString(SSE_KMS_KEY_ID_CONFIG);
  }

  public boolean useExpectContinue() {
    return getBoolean(HEADERS_USE_EXPECT_CONTINUE_CONFIG);
  }

  public CannedAccessControlList getCannedAcl() {
    return CannedAclValidator.ACLS_BY_HEADER_VALUE.get(getString(ACL_CANNED_CONFIG));
  }

  public int getPartSize() {
    return getInt(PART_SIZE_CONFIG);
  }

  @SuppressWarnings("unchecked")
  public AWSCredentialsProvider getCredentialsProvider() {
    try {
      AWSCredentialsProvider provider = ((Class<? extends AWSCredentialsProvider>)
          getClass(S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG)).newInstance();

      if (provider instanceof Configurable) {
        Map<String, Object> configs = originalsWithPrefix(CREDENTIALS_PROVIDER_CONFIG_PREFIX);
        configs.remove(CREDENTIALS_PROVIDER_CLASS_CONFIG.substring(
            CREDENTIALS_PROVIDER_CONFIG_PREFIX.length(),
            CREDENTIALS_PROVIDER_CLASS_CONFIG.length()
        ));
        ((Configurable) provider).configure(configs);
      }

      return provider;
    } catch (IllegalAccessException | InstantiationException e) {
      throw new ConnectException(
          "Invalid class for: " + S3SinkConnectorConfig.CREDENTIALS_PROVIDER_CLASS_CONFIG,
          e
      );
    }
  }

  public CompressionType getCompressionType() {
    return CompressionType.forName(getString(COMPRESSION_TYPE_CONFIG));
  }

  public CompressionCodecName getCompressionCodecName() {
    return CompressionCodecName.fromConf(null);
  }

  public int getS3PartRetries() {
    return getInt(S3_PART_RETRIES_CONFIG);
  }

  public String getByteArrayExtension() {
    return getString(FORMAT_BYTEARRAY_EXTENSION_CONFIG);
  }

  public String getFormatByteArrayLineSeparator() {
    // White space is significant for line separators, but ConfigKey trims it out,
    // so we need to check the originals rather than using the normal machinery.
    if (originalsStrings().containsKey(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG)) {
      return originalsStrings().get(FORMAT_BYTEARRAY_LINE_SEPARATOR_CONFIG);
    }
    return FORMAT_BYTEARRAY_LINE_SEPARATOR_DEFAULT;
  }

  protected static String parseName(Map<String, String> props) {
    String nameProp = props.get("name");
    return nameProp != null ? nameProp : "S3-sink";
  }

  public String getName() {
    return name;
  }

  @Override
  public Object get(String key) {
    ComposableConfig config = propertyToConfig.get(key);
    if (config == null) {
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    }
    return config == this ? super.get(key) : config.get(key);
  }

  public Map<String, ?> plainValues() {
    Map<String, Object> map = new HashMap<>();
    for (AbstractConfig config : allConfigs) {
      map.putAll(config.values());
    }
    return map;
  }

  private static class PartRange implements ConfigDef.Validator {
    // S3 specific limit
    final int min = 5 * 1024 * 1024;
    // Connector specific
    final int max = Integer.MAX_VALUE;

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null) {
        throw new ConfigException(name, value, "Part size must be non-null");
      }
      Number number = (Number) value;
      if (number.longValue() < min) {
        throw new ConfigException(
            name,
            value,
            "Part size must be at least: " + min + " bytes (5MB)"
        );
      }
      if (number.longValue() > max) {
        throw new ConfigException(
            name,
            value,
            "Part size must be no more: " + Integer.MAX_VALUE + " bytes (~2GB)"
        );
      }
    }

    public String toString() {
      return "[" + min + ",...," + max + "]";
    }
  }

  private static class RegionRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return Arrays.<Object>asList(RegionUtils.getRegions());
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  private static class RegionValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object region) {
      String regionStr = ((String) region).toLowerCase().trim();
      if (RegionUtils.getRegion(regionStr) == null) {
        throw new ConfigException(
            name,
            region,
            "Value must be one of: " + Utils.join(RegionUtils.getRegions(), ", ")
        );
      }
    }

    @Override
    public String toString() {
      return "[" + Utils.join(RegionUtils.getRegions(), ", ") + "]";
    }
  }

  private static class CompressionTypeValidator implements ConfigDef.Validator {
    public static final Map<String, CompressionType> TYPES_BY_NAME = new HashMap<>();
    public static final String ALLOWED_VALUES;

    static {
      List<String> names = new ArrayList<>();
      for (CompressionType compressionType : CompressionType.values()) {
        TYPES_BY_NAME.put(compressionType.name, compressionType);
        names.add(compressionType.name);
      }
      ALLOWED_VALUES = Utils.join(names, ", ");
    }

    @Override
    public void ensureValid(String name, Object compressionType) {
      String compressionTypeString = ((String) compressionType).trim();
      if (!TYPES_BY_NAME.containsKey(compressionTypeString)) {
        throw new ConfigException(name, compressionType, "Value must be one of: " + ALLOWED_VALUES);
      }
    }

    @Override
    public String toString() {
      return "[" + ALLOWED_VALUES + "]";
    }
  }

  private static class CannedAclValidator implements ConfigDef.Validator {
    public static final Map<String, CannedAccessControlList> ACLS_BY_HEADER_VALUE = new HashMap<>();
    public static final String ALLOWED_VALUES;

    static {
      List<String> aclHeaderValues = new ArrayList<>();
      for (CannedAccessControlList acl : CannedAccessControlList.values()) {
        ACLS_BY_HEADER_VALUE.put(acl.toString(), acl);
        aclHeaderValues.add(acl.toString());
      }
      ALLOWED_VALUES = Utils.join(aclHeaderValues, ", ");
    }

    @Override
    public void ensureValid(String name, Object cannedAcl) {
      if (cannedAcl == null) {
        return;
      }
      String aclStr = ((String) cannedAcl).trim();
      if (!ACLS_BY_HEADER_VALUE.containsKey(aclStr)) {
        throw new ConfigException(name, cannedAcl, "Value must be one of: " + ALLOWED_VALUES);
      }
    }

    @Override
    public String toString() {
      return "[" + ALLOWED_VALUES + "]";
    }
  }

  private static class CredentialsProviderValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object provider) {
      if (provider != null && provider instanceof Class
              && AWSCredentialsProvider.class.isAssignableFrom((Class<?>) provider)) {
        return;
      }
      throw new ConfigException(
          name,
          provider,
          "Class must extend: " + AWSCredentialsProvider.class
      );
    }

    @Override
    public String toString() {
      return "Any class implementing: " + AWSCredentialsProvider.class;
    }
  }

  private static class SseAlgorithmRecommender implements ConfigDef.Recommender {
    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      List<SSEAlgorithm> list = Arrays.asList(SSEAlgorithm.values());
      return new ArrayList<Object>(list);
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return true;
    }
  }

  public static class SseKmsKeyIdRecommender implements ConfigDef.Recommender {
    public SseKmsKeyIdRecommender() {
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
      return new LinkedList<>();
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
      return SSEAlgorithm.KMS.toString()
          .equalsIgnoreCase((String) connectorConfigs.get(SSEA_CONFIG));
    }
  }

  public static ConfigDef getConfig() {
    // Define the names of the configurations we're going to override
    Set<String> skip = new HashSet<>();
    skip.add(StorageSinkConnectorConfig.SHUTDOWN_TIMEOUT_CONFIG);

    // Order added is important, so that group order is maintained
    ConfigDef visible = new ConfigDef();
    addAllConfigKeys(visible, newConfigDef(), skip);
    addAllConfigKeys(visible, StorageCommonConfig.newConfigDef(STORAGE_CLASS_RECOMMENDER), skip);
    addAllConfigKeys(visible, PartitionerConfig.newConfigDef(PARTITIONER_CLASS_RECOMMENDER), skip);

    return visible;
  }

  private static void addAllConfigKeys(ConfigDef container, ConfigDef other, Set<String> skip) {
    for (ConfigDef.ConfigKey key : other.configKeys().values()) {
      if (skip != null && !skip.contains(key.name)) {
        container.define(key);
      }
    }
  }

  public String nullValueBehavior() {
    return getString(BEHAVIOR_ON_NULL_VALUES_CONFIG);
  }

  public enum BehaviorOnNullValues {
    IGNORE,
    FAIL;

    public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
      private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

      @Override
      public void ensureValid(String name, Object value) {
        if (value instanceof String) {
          value = ((String) value).toLowerCase(Locale.ROOT);
        }
        validator.ensureValid(name, value);
      }

      // Overridden here so that ConfigDef.toEnrichedRst shows possible values correctly
      @Override
      public String toString() {
        return validator.toString();
      }

    };

    public static String[] names() {
      BehaviorOnNullValues[] behaviors = values();
      String[] result = new String[behaviors.length];

      for (int i = 0; i < behaviors.length; i++) {
        result[i] = behaviors[i].toString();
      }

      return result;
    }

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public static void main(String[] args) {
    System.out.println(getConfig().toEnrichedRst());
  }

}
