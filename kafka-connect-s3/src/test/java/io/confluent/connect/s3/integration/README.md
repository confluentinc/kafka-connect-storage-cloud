## Integration Test Setup

The following resources need to be setup before running integration tests. 

### AWS Credentials
The integration tests follow the `DefaultAWSCredentialsProviderChain` for S3 authentication. 
One of the simplest ways to ensure access to S3 for these tests is by configuring 
the `.aws/credentials` file. 

### S3 Bucket
The integration tests expect a test bucket to already exist, 
by default the bucket name is `confluent-kafka-connect-s3-testing` which 
can be changed in the `BaseConnectorIT` class. 

## Running the Tests
By default the tests are ignored when the project is built, they have to be run manually.
This can be done using the `mvn integration-test` command
or through IntelliJ by running the class.