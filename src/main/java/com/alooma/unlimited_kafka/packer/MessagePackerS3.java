package com.alooma.unlimited_kafka.packer;

import com.alooma.unlimited_kafka.Capsule;
import com.alooma.unlimited_kafka.Serializer;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class MessagePackerS3<T> implements MessagePacker<T> {

    private S3Client s3;
    private String bucket;
    private Serializer<T> serializer;
    private long byteSizeThreshold;

    public MessagePackerS3(Region region, String bucket, long byteSizeThreshold,
                    Serializer<T> serializer,
                    AwsCredentialsProvider provider) {
        this.s3 = S3Client.builder().credentialsProvider(provider).region(region).build();
        this.bucket = bucket;
        this.byteSizeThreshold = byteSizeThreshold;
        this.serializer = serializer;
    }

    public MessagePackerS3(Region region, String bucket, long byteSizeThreshold,Serializer<T> serializer) {
        this(region,bucket,byteSizeThreshold, serializer, DefaultCredentialsProvider.create());
    }

    public MessagePackerS3(S3Client s3Client,
                           String bucket,
                           long byteSizeThreshold,
                           Serializer<T> serializer) {
        this.s3 = s3Client;
        this.bucket = bucket;
        this.serializer = serializer;
        this.byteSizeThreshold = byteSizeThreshold;
    }

    public Capsule<T> packMessage(T message, String topic, Long offset) {

        String key = String.format("%s/%d", topic, offset);

        // Put Object
        byte[] serializedBytes = serializer.serialize(message);

        if (serializedBytes.length > byteSizeThreshold) {
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key)
                            .build(),
                    RequestBody.fromBytes(serializedBytes));

            return Capsule.remoteCapsule(key);
        } else {
            return Capsule.localCapsule(message);
        }
    }
}
