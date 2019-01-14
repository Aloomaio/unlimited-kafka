package com.alooma.unlimited_kafka.packer.s3;

import java.time.format.DateTimeFormatter;

public class S3ManagerParamsBuilder {

    private S3ManagerParams s3ManagerParams = new S3ManagerParams();

    public S3ManagerParamsBuilder withMultipartUploadThreshold(long multipartUploadThreshold){
        this.s3ManagerParams.setMultipartUploadThreshold(multipartUploadThreshold);
        return this;
    }

    public S3ManagerParamsBuilder withMinimumUploadPartSize(long minimumUploadPartSize){
        this.s3ManagerParams.setMinimumUploadPartSize(minimumUploadPartSize);
        return this;
    }

    public S3ManagerParamsBuilder withThreadPoolSize(int threadPoolSize){
        this.s3ManagerParams.setThreadPoolSize(threadPoolSize);
        return this;
    }

    public S3ManagerParamsBuilder withDirectoryNamePrefix(String directoryNamePrefix){
        this.s3ManagerParams.setDirectoryNamePrefix(directoryNamePrefix);
        return this;
    }

    public S3ManagerParamsBuilder withDirectoryNameDateTimeFormatter(DateTimeFormatter dateTimeFormatter){
        this.s3ManagerParams.setDateTimeFormatter(dateTimeFormatter);
        return this;
    }

    public S3ManagerParams build(){
        return s3ManagerParams;
    }
}
