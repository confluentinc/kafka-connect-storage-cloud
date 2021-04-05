## Integration Test Setup

The following resources need to be setup to run integration tests with a real S3 instance. 

### AWS Credentials

The integration tests follow the `DefaultAWSCredentialsProviderChain` for S3 authentication. 
One of the simplest ways to test locally is to configure the `.aws/credentials` file.
You can also define environment variable as described [here](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html)

We support defining `AWS_CREDENTIALS_PATH` to point a json format credential file. This is used in Jenkins.
e.g. the following script will create the json file using the known environment variables.

```
cat << EOF > s3_credentials.json
{
"aws_access_key_id": "$AWS_ACCESS_KEY_ID",
"aws_secret_access_key": "$AWS_SECRET_ACCESS_KEY"
}
EOF
```

## Running the Tests
Tests can be run manually using the `mvn integration-test` command 
or through IntelliJ by running the class.