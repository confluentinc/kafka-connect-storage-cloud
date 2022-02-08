package io.confluent.connect.s3.util;

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class EmbeddedConnectUtils {
    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnectUtils.class);

    public static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.MINUTES.toMillis(1);

    /**
     * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the
     * given name to start the specified number of tasks.
     *
     * @param name     the name of the connector
     * @param numTasks the minimum number of tasks that are expected
     * @return the time this method discovered the connector has started, in milliseconds past epoch
     * @throws InterruptedException if this was interrupted
     */
    public static long waitForConnectorToStart(EmbeddedConnectCluster connect, String name, int numTasks)
            throws InterruptedException {
        TestUtils.waitForCondition(
                () -> assertConnectorAndTasksRunning(connect, name, numTasks).orElse(false),
                CONNECTOR_STARTUP_DURATION_MS,
                "Connector tasks did not start in time."
        );
        return System.currentTimeMillis();
    }

    /**
     * Confirm that a connector with an exact number of tasks is running.
     *
     * @param connectorName the connector
     * @param numTasks      the minimum number of tasks
     * @return true if the connector and tasks are in RUNNING state; false otherwise
     */
    private static Optional<Boolean> assertConnectorAndTasksRunning(EmbeddedConnectCluster connect, String connectorName, int numTasks) {
        try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            boolean result = info != null
                    && info.tasks().size() >= numTasks
                    && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                    && info.tasks().stream()
                    .allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
            return Optional.of(result);
        } catch (Exception e) {
            log.warn("Could not check connector state info.");
            return Optional.empty();
        }
    }
}
