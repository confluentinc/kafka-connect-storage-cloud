package io.confluent.connect.s3;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.After;
import org.junit.Rule;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;

public class TestWithMockedFaultyS3 extends TestWithMockedS3 {
    // move S3 mock to another port
    protected final static int MOCKED_S3_PORT = 8182;
    protected final static String MOCKED_S3_URL = "http://localhost:" + MOCKED_S3_PORT;

    // replace original S3 mock port by a WireMock proxy
    protected final static int PROXIED_S3_PORT = Integer.parseInt(S3_TEST_URL.substring(S3_TEST_URL.lastIndexOf(":") + 1));

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PROXIED_S3_PORT);

    //@Before
    @Override
    public void setUp() throws Exception {
        port = String.valueOf(MOCKED_S3_PORT);
        super.setUp();

        // propagate all requests to S3 mock by default
        stubFor(any(anyUrl()).atPriority(10).willReturn(aResponse().proxiedFrom(MOCKED_S3_URL)));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Inject the specified failure for two times.
     * Third and following requests will go to mocked S3 instance.
     */
    protected static void injectS3FailureFor(MappingBuilder mapping) {
        stubFor(mapping
                .atPriority(1)
                .inScenario("TestWithMockedFaultyS3")
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("SECOND_FAILURE")
        );

        stubFor(mapping
                .atPriority(1)
                .inScenario("TestWithMockedFaultyS3")
                .whenScenarioStateIs("SECOND_FAILURE")
                .willSetStateTo("RECOVER")
        );

        stubFor(mapping
                .atPriority(1)
                .inScenario("TestWithMockedFaultyS3")
                .whenScenarioStateIs("RECOVER")
                .willReturn(
                        aResponse().proxiedFrom(MOCKED_S3_URL) // success
                )
        );
    }
}
