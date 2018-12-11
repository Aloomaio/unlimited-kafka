package com.alooma.unlimited_kafka.unpacker;

import com.alooma.unlimited_kafka.Capsule;
import com.alooma.unlimited_kafka.SerializeableFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MessageUnpackerS3<T> implements MessageUnpacker<T> {

    private S3Client s3;
    private String bucket;
    private SerializeableFactory<T> factory;


    public MessageUnpackerS3(Region region, String bucket,
                      SerializeableFactory<T> factory,
                      AwsCredentialsProvider provider) {
        this.s3 = S3Client.builder().credentialsProvider(provider).region(region).build();
        this.bucket = bucket;
        this.factory = factory;
    }

    public MessageUnpackerS3(Region region, String bucket,
                      SerializeableFactory<T> factory) {
        this(region, bucket, factory, DefaultCredentialsProvider.create());
    }

    public MessageUnpackerS3(S3Client s3,
                             String bucket,
                             SerializeableFactory<T> factory) {
        this.s3 = s3;
        this.bucket = bucket;
        this.factory = factory;
    }

    @Override
    public T unpackMessage(Capsule<T> capsule) {

        if (capsule.getType() == Capsule.Type.REMOTE) {
            String key = capsule.getKey();
            return unpack(key);
        } else if (capsule.getType() == Capsule.Type.LOCAL) {
            return capsule.getData();
        } else {
            throw new NotImplementedException();
        }
    }

    private T unpack(String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
        byte[] objectAsBytes = s3.getObjectAsBytes(getObjectRequest).asByteArray();
        return factory.fromBytes(objectAsBytes);
    }
}
