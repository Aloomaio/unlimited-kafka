package com.alooma.unlimited_kafka.packer;

import com.alooma.unlimited_kafka.Capsule;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;


class MessagePackerS3Test {

    @Test
    void testPackLocal() {
        S3Client s3 = mock(S3Client.class);

        MessagePackerS3<String> packer = new MessagePackerS3<>(s3, "maor-test-retention", 1000, String::getBytes);

        Capsule<String> capsule = packer.packMessage("message", "topic", 123L);

        assertEquals(capsule.getType(), Capsule.Type.LOCAL);
        assertNull(capsule.getKey());
        assertEquals(capsule.getData(), "message");
    }

    @Test
    void testPackRemote() {
        S3Client s3 = mock(S3Client.class);

        MessagePackerS3<String> packer = new MessagePackerS3<>(s3, "maor-test-retention", 1, String::getBytes);

        Capsule<String> capsule = packer.packMessage("message", "topic", 123L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertEquals(capsule.getKey(), "topic/123");
        assertNull(capsule.getData());
    }

    @Test
    void testConstructor() {
        MessagePackerS3<String> packerS3 = new MessagePackerS3<>(Region.EU_WEST_1, "bucket", 100000L, String::getBytes);

        assertEquals(Capsule.localCapsule("fakemessage"), packerS3.packMessage("fakemessage", "topic", 123L));
    }
}