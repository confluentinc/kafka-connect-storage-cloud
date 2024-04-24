package io.confluent.connect.s3.auth.iamAssume;

import com.amazonaws.auth.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Tag;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static io.confluent.connect.s3.S3SinkConnectorConfig.CUSTOMER_ROLE_ARN_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.CUSTOMER_ROLE_EXTERNAL_ID_CONFIG;
import static io.confluent.connect.s3.S3SinkConnectorConfig.MIDDLEWARE_ROLE_ARN_CONFIG;
import java.util.Map;

public class AwsIAMAssumeRoleChaining implements AWSCredentialsProvider {

    private static final ConfigDef STS_CONFIG_DEF = new ConfigDef()
        .define(
            CUSTOMER_ROLE_EXTERNAL_ID_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.MEDIUM,
            "The role external ID used when retrieving session credentials under an assumed role."
        ).define(
            CUSTOMER_ROLE_ARN_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Role ARN to use when starting a session."
        ).define(
            MIDDLEWARE_ROLE_ARN_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Role ARN to use when starting a session."
        );

    private String customerRoleArn;
    private String customerRoleExternalId;
    private String middlewareRoleArn;
    private STSAssumeRoleSessionCredentialsProvider stsCredentialProvider;

    // Method to initiate role chaining
    public void configure(Map<String, ?> configs) {
        // Assume the initial role
        AbstractConfig config = new AbstractConfig(STS_CONFIG_DEF, configs);
        customerRoleArn = config.getString(CUSTOMER_ROLE_ARN_CONFIG);
        customerRoleExternalId = config.getString(CUSTOMER_ROLE_EXTERNAL_ID_CONFIG);
        middlewareRoleArn = config.getString(MIDDLEWARE_ROLE_ARN_CONFIG);

        STSAssumeRoleSessionCredentialsProvider initialProvider = buildProvider(middlewareRoleArn, "middlewareSession", "", null);

        // Use the credentials from the initial role to assume the subsequent role
        stsCredentialProvider = buildProvider(customerRoleArn, "customerSession", customerRoleExternalId, initialProvider);
    }

    // Updated buildProvider to optionally accept an existing AwsCredentialsProvider
    private STSAssumeRoleSessionCredentialsProvider buildProvider(final String roleArn, final String roleSessionName, final String roleExternalId, STSAssumeRoleSessionCredentialsProvider existingProvider) {
        STSAssumeRoleSessionCredentialsProvider credentialsProvider;
        // If an existing credentials provider is provided, use it for creating the STS client
        if (existingProvider != null) {
            AWSCredentials basicCredentials = existingProvider.getCredentials();
            credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
                .Builder(roleArn, roleSessionName)
                .withStsClient(AWSSecurityTokenServiceClientBuilder
                        .standard()
                        .withCredentials(new AWSStaticCredentialsProvider(basicCredentials)).build()
                )
                .withExternalId(roleExternalId)
                .build();
        } else {
            credentialsProvider = new STSAssumeRoleSessionCredentialsProvider
                .Builder(roleArn, roleSessionName)
                .withStsClient(AWSSecurityTokenServiceClientBuilder.defaultClient())
                .withExternalId(roleExternalId)
                .build();
        }
        return credentialsProvider;
    }

    // Helper method to build the AssumeRoleRequest
    private AssumeRoleRequest buildRequest(String roleExternalId, String roleArn) {
        return AssumeRoleRequest.builder()
            .roleArn(roleArn)
            .externalId(roleExternalId)
            .build();
    }

    @Override
    public AWSCredentials getCredentials() {
        return stsCredentialProvider.getCredentials();
    }

    @Override
    public void refresh() {
        stsCredentialProvider.refresh();
    }
}
