package io.confluent.connect.s3.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * AWS credentials provider that uses the AWS IAM Roles for Service Accounts (IRSA) to assume a Role
 * and create a temporary, short-lived session to use for authentication.
 */
public class AwsWebIdentityTokenCredentialsProvider implements AWSCredentialsProvider,
    Configurable {

  public static final String ROLE_ARN_CONFIG = "irsa.role.arn";
  public static final String ROLE_SESSION_NAME_CONFIG = "irsa.session.name";
  public static final String WEB_IDENTITY_TOKEN_FILE_CONFIG = "irsa.token.file";

  private WebIdentityTokenCredentialsProvider webIdentityTokenCredentialsProvider;

  private static final ConfigDef CONFIG_DEF = createConfigDef();

  private static ConfigDef createConfigDef() {
    return new ConfigDef()
        .define(
            ROLE_ARN_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Role ARN to use when starting a session."
        ).define(
            ROLE_SESSION_NAME_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Role session name to use when starting a session."
        ).define(
            WEB_IDENTITY_TOKEN_FILE_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Path to the web identity token file.")
        ;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);

    String roleArn = config.getString(ROLE_ARN_CONFIG);
    String roleSessionName = config.getString(ROLE_SESSION_NAME_CONFIG);
    String tokenFile = config.getString(WEB_IDENTITY_TOKEN_FILE_CONFIG);

    webIdentityTokenCredentialsProvider = com.amazonaws.auth.WebIdentityTokenCredentialsProvider.builder()
        .roleArn(roleArn)
        .roleSessionName(roleSessionName)
        .webIdentityTokenFile(tokenFile)
        .build();
  }

  @Override
  public AWSCredentials getCredentials() {
    if (webIdentityTokenCredentialsProvider == null) {
      throw new IllegalStateException(
          "WebIdentityTokenCredentialsProvider has not been configured.");
    }
    return webIdentityTokenCredentialsProvider.getCredentials();
  }

  @Override
  public void refresh() {
    if (webIdentityTokenCredentialsProvider != null) {
      webIdentityTokenCredentialsProvider.refresh();
    }
  }
}
