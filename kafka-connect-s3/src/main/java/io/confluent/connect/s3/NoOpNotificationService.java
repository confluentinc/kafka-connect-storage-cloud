package io.confluent.connect.s3;

import io.confluent.connect.s3.NotificationService;
import s3connect.FileUploadedMessage;

public class NoOpNotificationService implements NotificationService {

    @Override
    public void send(FileUploadedMessage message) {
    }

    @Override
    public void close() {
    }
}
