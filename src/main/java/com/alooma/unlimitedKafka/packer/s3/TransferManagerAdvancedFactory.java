package com.alooma.unlimitedKafka.packer.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import java.util.concurrent.Executors;

public class TransferManagerAdvancedFactory {

    public TransferManager create(AmazonS3 amazonS3, S3ManagerParams s3ManagerParams) {

        TransferManagerBuilder builder = TransferManagerBuilder.standard().withS3Client(amazonS3);

        s3ManagerParams.getOptionalOfMinimumUploadPartSize().ifPresent(builder::withMinimumUploadPartSize);
        s3ManagerParams.getOptionalOfMultipartUploadThreshold().ifPresent(builder::withMultipartUploadThreshold);
        s3ManagerParams.getOptionalOfThreadPoolSize()
                .ifPresent(poolSize -> builder.withExecutorFactory(() -> Executors.newFixedThreadPool(poolSize)));

        return builder.build();
    }
}
