package com.alooma.unlimited_kafka.packer.s3;

import com.alooma.unlimited_kafka.Capsule;
import com.alooma.unlimited_kafka.Serializer;
import com.alooma.unlimited_kafka.packer.MessagePacker;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class MessagePackerS3<T> implements MessagePacker<T> {

    private AmazonS3 s3Client;
    private String bucket;
    private Serializer<T> serializer;
    private long byteSizeThreshold;
    private S3ManagerParams s3ManagerParams;
    private TransferManager transferManager;

    public MessagePackerS3(Regions region,
                           String bucket,
                           long byteSizeThreshold,
                           Serializer<T> serializer,
                           AWSCredentialsProvider provider,
                           S3ManagerParams s3ManagerParams) {
        this.s3Client = AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(provider).build();
        this.bucket = bucket;
        this.byteSizeThreshold = byteSizeThreshold;
        this.serializer = serializer;
        this.s3ManagerParams = s3ManagerParams;
    }

    public MessagePackerS3(Regions region, String bucket, long byteSizeThreshold, Serializer<T> serializer) {
        this(region, bucket, byteSizeThreshold, serializer,
                new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()), new S3ManagerParams());
    }

    public MessagePackerS3(AmazonS3 s3Client,
                           String bucket,
                           long byteSizeThreshold,
                           Serializer<T> serializer,
                           S3ManagerParams s3ManagerParams) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.serializer = serializer;
        this.byteSizeThreshold = byteSizeThreshold;
        this.s3ManagerParams = s3ManagerParams;
    }

    public Capsule<T> packMessage(T message, String topic, Long offset) {

        byte[] serializedBytes = serializer.serialize(message);
        String key = createKey(topic, offset, s3ManagerParams.isShouldUploadAsGzip());
        if (serializedBytes.length > byteSizeThreshold) {
            try {
                Upload upload = upload(serializedBytes, key);
                upload.waitForCompletion();
                if (upload.isDone()) {
                    System.out.println("Object upload complete");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                transferManager.shutdownNow(false);
            }
            return Capsule.remoteCapsule(key);
        }
        return Capsule.localCapsule(message);
    }

    private String createKey(String topic, Long offset, boolean shouldUploadAsGz) {
        String key = String.format("%s/%d", topic, offset);
        if (shouldUploadAsGz){
            return key.concat(".gz");
        }
        return key;
    }

    private Upload upload(byte[] serializedBytes, String key) throws IOException {
        transferManager = new TransferManagerAdvancedFactory().create(s3Client, s3ManagerParams);
        byte[] inputBytes = s3ManagerParams.isShouldUploadAsGzip() ? getGzipBytes(serializedBytes) : serializedBytes;
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(inputBytes.length);
        return transferManager.upload(bucket, key, new ByteArrayInputStream(inputBytes), metadata);
    }

    private byte[] getGzipBytes(byte[] serializedBytes) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        gzipOutputStream.write(serializedBytes);
        gzipOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }
}

