package com.alooma.unlimited_kafka.packer.s3;

import com.alooma.unlimited_kafka.packer.StorageManagerParams;

import java.util.Optional;

public class S3ManagerParams implements StorageManagerParams {

    private Long multipartUploadThreshold;
    private Long minimumUploadPartSize;
    private Integer threadPoolSize;

    public Long getMultipartUploadThreshold() {
        return multipartUploadThreshold;
    }

    public void setMultipartUploadThreshold(long multipartUploadThreshold) {
        this.multipartUploadThreshold = multipartUploadThreshold;
    }

    public Long getMinimumUploadPartSize() {
        return minimumUploadPartSize;
    }

    public void setMinimumUploadPartSize(long minimumUploadPartSize) {
        this.minimumUploadPartSize = minimumUploadPartSize;
    }

    public Integer getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public Optional<Integer> getOptionalOfThreadPoolSize(){
        return Optional.ofNullable(threadPoolSize);

    }

    public Optional<Long> getOptionalOfMultipartUploadThreshold() {
        return Optional.ofNullable(multipartUploadThreshold);
    }


    public Optional<Long> getOptionalOfMinimumUploadPartSize() {
        return Optional.ofNullable(minimumUploadPartSize);
    }

}
