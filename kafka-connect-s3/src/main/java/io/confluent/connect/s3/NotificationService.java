package io.confluent.connect.s3;

import s3connect.FileUploadedMessage;

public interface NotificationService {
    // send file uploaded message
    void send(FileUploadedMessage message);

    // close the Notification Service
    void close();


}
