package com.alooma.unlimited_kafka.packer.s3;

public class S3ManagerParamsBuilder {

    private S3ManagerParams s3ManagerParams = new S3ManagerParams();

    public S3ManagerParamsBuilder addMultipartUploadThreshold(long multipartUploadThreshold){
        this.s3ManagerParams.setMultipartUploadThreshold(multipartUploadThreshold);
        return this;
    }

    public S3ManagerParamsBuilder addMinimumUploadPartSize(long minimumUploadPartSize){
        this.s3ManagerParams.setMinimumUploadPartSize(minimumUploadPartSize);
        return this;
    }

    public S3ManagerParamsBuilder addThreadPoolSize(int threadPoolSize){
        this.s3ManagerParams.setThreadPoolSize(threadPoolSize);
        return this;
    }

    public S3ManagerParamsBuilder addShouldUploadAsGzip(Boolean shouldUploadAsGzip){
        this.s3ManagerParams.setShouldUploadAsGzip(shouldUploadAsGzip);
        return this;
    }

    public S3ManagerParams build(){
        return s3ManagerParams;
    }
}
