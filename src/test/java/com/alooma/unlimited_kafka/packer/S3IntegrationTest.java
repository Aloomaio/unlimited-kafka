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
import org.joda.time.format.DateTimeFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3IntegrationTest {

    private static final String bucket = "testbucket";
    private final Pattern defaultPatternWithSuffix = Pattern.compile(".*\\d{4}/\\d{2}/\\d{2}/\\d{2}/.*/[a-z0-9\\-]{36}\\.gz");
    private final Pattern defaultPattern = Pattern.compile(".*\\d{4}/\\d{2}/\\d{2}/\\d{2}/.*/[a-z0-9\\-]{36}");
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
    void testS3MultipartUpload() {
        S3ManagerParams s3ManagerParams = new S3ManagerParamsBuilder()
                .addShouldUploadAsGzip(true)
                .build();
        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 1, String::getBytes, s3ManagerParams);
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertTrue(defaultPatternWithSuffix.matcher(capsule.getKey()).matches());
    }

    @Test
    void testS3UploadFileAsGz() {
        S3ManagerParams s3ManagerParams = new S3ManagerParamsBuilder()
                .addShouldUploadAsGzip(true)
                .build();
        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 1, String::getBytes, s3ManagerParams);
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertTrue(defaultPatternWithSuffix.matcher(capsule.getKey()).matches());
    }

    @Test
    void testS3UploadFileNotAsGz() {
        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 1, String::getBytes, new S3ManagerParams());
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertTrue(defaultPattern.matcher(capsule.getKey()).matches());
    }

    @Test
    void testS3MultipartUpload_withS3Params() {
        S3ManagerParams s3ManagerParams = new S3ManagerParamsBuilder()
                .withMultipartUploadThreshold(6000000L)
                .withMinimumUploadPartSize(10000L)
                .withThreadPoolSize(1)
                .withDirectoryNamePrefix("usa")
                .addShouldUploadAsGzip(true)
                .withDirectoryNameDateTimeFormatter(DateTimeFormatter.ofPattern("yyyy'/'MM'/'dd'/'HH"))
                .build();

        MessagePackerS3<String> packerS3 = new MessagePackerS3<String>(client, bucket, 1, String::getBytes, s3ManagerParams);
        Capsule<String> capsule = packerS3.packMessage("testMultipartUpload", "test1", 12L);

        assertEquals(capsule.getType(), Capsule.Type.REMOTE);
        assertTrue(defaultPatternWithSuffix.matcher(capsule.getKey()).matches());
    }

    @Test
    void testS3SingleUpload(){
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
