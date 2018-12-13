package com.alooma.unlimited_kafka.unpacker;

import com.alooma.unlimited_kafka.Capsule;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageUnpackerS3Test {

    @Test
    void unpackMessageLocal() {
        S3Client s3 = mock(S3Client.class);

        MessageUnpackerS3<String> unpacker = new MessageUnpackerS3<>(s3, "bucket", String::new);

        String mockData = "local capsule data";
        assertEquals(
                unpacker.unpackMessage(Capsule.localCapsule(mockData)),
                mockData
        );

    }

    @Test
    void unpackMessageRemote() {
        S3Client s3 = mock(S3Client.class, RETURNS_DEEP_STUBS);

        MessageUnpackerS3<String> unpacker = new MessageUnpackerS3<>(s3, "bucket", String::new);

        String mockData = "local capsule data";

        when(s3.getObjectAsBytes(any(GetObjectRequest.class)).asByteArray()).thenReturn(mockData.getBytes());
        assertEquals(
                unpacker.unpackMessage(Capsule.remoteCapsule("topic/000")),
                mockData
        );
    }

    @Test
    void testConstructor() {
        MessageUnpackerS3<String> unpackerS3 = new MessageUnpackerS3<>(Region.EU_WEST_1, "bucket", String::new);

        assertEquals("fakedata", unpackerS3.unpackMessage(Capsule.localCapsule("fakedata")));

    }

}