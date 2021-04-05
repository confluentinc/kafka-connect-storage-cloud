## Integration Test Setup

The following resources need to be setup to run integration tests with a real S3 instance. 

### AWS Credentials
The integration tests follow the `DefaultAWSCredentialsProviderChain` for S3 authentication. 
One of the simplest ways to ensure access to S3 for these tests is by configuring 
the `.aws/credentials` file. 

## Running the Tests
Tests can be run manually using the `mvn integration-test` command 
or through IntelliJ by running the class.