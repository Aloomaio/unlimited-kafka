package com.alooma.unlimited_kafka.unpacker;

import com.alooma.unlimited_kafka.Capsule;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MessageUnpackerS3Test {

    @Test
    void unpackMessageLocal() {
        AmazonS3 s3 = mock(AmazonS3.class);

        MessageUnpackerS3<String> unpacker = new MessageUnpackerS3<>(s3, "bucket", String::new);

        String mockData = "local capsule data";
        assertEquals(
                unpacker.unpackMessage(Capsule.localCapsule(mockData)),
                mockData
        );
    }

    @Test
    void unpackMessageRemote() {
        AmazonS3 s3 = mock(AmazonS3.class, RETURNS_DEEP_STUBS);
        S3Object s3Object = mock(S3Object.class);
        MessageUnpackerS3<String> unpacker = new MessageUnpackerS3<>(s3, "bucket", String::new);

        String mockData = "local capsule data";
        byte[] expectedBytes = mockData.getBytes();
        when(s3.getObject(any(GetObjectRequest.class))).thenReturn(s3Object);
        when(s3Object.getObjectContent())
                .thenReturn(new S3ObjectInputStream(new ByteArrayInputStream(expectedBytes), null));

        assertEquals(
                unpacker.unpackMessage(Capsule.remoteCapsule("topic/000")),
                mockData
        );
    }

    @Test
    void testConstructor() {
        MessageUnpackerS3<String> unpackerS3 = new MessageUnpackerS3<>(Regions.EU_WEST_1, "bucket", String::new);

        assertEquals("fakedata", unpackerS3.unpackMessage(Capsule.localCapsule("fakedata")));

    }
}