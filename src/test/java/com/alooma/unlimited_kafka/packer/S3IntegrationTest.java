package com.alooma.unlimited_kafka.packer;

import com.alooma.unlimited_kafka.Capsule;
import com.alooma.unlimited_kafka.packer.s3.MessagePackerS3;
import com.alooma.unlimited_kafka.packer.s3.S3ManagerParams;
import com.alooma.unlimited_kafka.packer.s3.S3ManagerParamsBuilder;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class S3IntegrationTest {


    private static final String bucket = "testbucket";
    private static AmazonS3 client;
    private static S3Mock api;

    @BeforeAll
    public static void init() {
        api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
        api.start();

        EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
        client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();

        client.createBucket(bucket);
    }


    @Test
    void testS3MultipartUpload() throws InterruptedException, IOException {

        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 1, String::getBytes, new S3ManagerParams());
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertEquals("test1/12.gz", capsule.getKey());
    }

    @Test
    void testS3UploadFileType() throws InterruptedException, IOException {

        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 1, String::getBytes, new S3ManagerParams());
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertEquals("test1/12.gz", capsule.getKey());
    }

    @Test
    void testS3MultipartUpload_withS3Params() throws InterruptedException, IOException {

        S3ManagerParams s3ManagerParams = new S3ManagerParamsBuilder()
                .addMultipartUploadThreshold(6000000L)
                .addMinimumUploadPartSize(10000L)
                .addThreadPoolSize(1)
                .build();

        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 1, String::getBytes, s3ManagerParams);
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertEquals("test1/12.gz", capsule.getKey());
    }

    @Test
    void testS3SingleUpload() throws InterruptedException, IOException {

        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 6000000L, String::getBytes, new S3ManagerParams());
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.LOCAL);
        assertEquals("testMultipartUpload", capsule.getData());
    }

    @AfterAll
    public static void cleanUp() {
        api.stop();
    }
}