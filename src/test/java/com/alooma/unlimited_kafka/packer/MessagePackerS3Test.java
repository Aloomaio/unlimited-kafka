package com.alooma.unlimited_kafka.packer;

import com.alooma.unlimited_kafka.Capsule;
import com.alooma.unlimited_kafka.packer.s3.MessagePackerS3;
import com.alooma.unlimited_kafka.packer.s3.S3ManagerParams;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;


class MessagePackerS3Test {

    @Test
    void testPackLocal() {
        AmazonS3 s3 = mock(AmazonS3.class);
        S3ManagerParams s3ManagerParams = mock(S3ManagerParams.class);

        MessagePackerS3<String> packer = new MessagePackerS3<>(s3, "maor-test-retention", 1000, String::getBytes, s3ManagerParams);

        Capsule<String> capsule = packer.packMessage("message", "topic", 123L);

        assertEquals(capsule.getType(), Capsule.Type.LOCAL);
        assertNull(capsule.getKey());
        assertEquals(capsule.getData(), "message");
    }


    @Test
    void testConstructor() {
        MessagePackerS3<String> packerS3 = new MessagePackerS3<>(Regions.EU_WEST_1, "bucket", 100000L, String::getBytes);

        assertEquals(Capsule.localCapsule("fakemessage"), packerS3.packMessage("fakemessage", "topic", 123L));
    }
}