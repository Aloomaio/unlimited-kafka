package com.alooma.unlimited_kafka.packer;

import com.alooma.unlimited_kafka.Capsule;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;


class MessagePackerS3Test {

    @Test
    public void testPackLocal() {
        S3Client s3 = mock(S3Client.class);

        MessagePackerS3<String> packer = new MessagePackerS3<>(s3, "maor-test-retention", 1000, String::getBytes);

        Capsule<String> capsule = packer.packMessage("message", "topic", 123L);

        assertEquals(capsule.getType(), Capsule.Type.LOCAL);
        assertNull(capsule.getKey());
        assertEquals(capsule.getData(), "message");
    }

    @Test
    public void testPackRemote() {
        S3Client s3 = mock(S3Client.class);

        MessagePackerS3<String> packer = new MessagePackerS3<>(s3, "maor-test-retention", 1, String::getBytes);

        Capsule<String> capsule = packer.packMessage("message", "topic", 123L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertEquals(capsule.getKey(), "topic/123");
        assertNull(capsule.getData());
    }


}