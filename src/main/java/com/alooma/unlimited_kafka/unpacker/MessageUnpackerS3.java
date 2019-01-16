package com.alooma.unlimited_kafka.unpacker;

import java.io.IOException;
import com.alooma.unlimited_kafka.Capsule;
import com.alooma.unlimited_kafka.SerializeableFactory;
import com.alooma.unlimited_kafka.exceptions.UnpackException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;

public class MessageUnpackerS3<T> implements MessageUnpacker<T> {

    private AmazonS3 s3;
    private String bucket;
    private SerializeableFactory<T> factory;

    public MessageUnpackerS3(Regions region, String bucket,
                             SerializeableFactory<T> factory,
                             AWSCredentialsProvider provider) {
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(provider).build();
        this.bucket = bucket;
        this.factory = factory;
    }

    public MessageUnpackerS3(Regions region, String bucket,
                             SerializeableFactory<T> factory) {
        this(region, bucket, factory, new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()));
    }

    public MessageUnpackerS3(AmazonS3 s3,
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
            throw new UnsupportedOperationException();
        }
    }

    private T unpack(String key) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, key);
        S3ObjectInputStream objectContent = s3.getObject(getObjectRequest).getObjectContent();
        byte[] objectAsBytes;
        try {
            objectAsBytes = IOUtils.toByteArray(objectContent);
        } catch (IOException e) {
            throw new UnpackException(e);
        }
        return factory.fromBytes(objectAsBytes);
    }
}
