package com.alooma.unlimited_kafka.packer.s3;

import com.alooma.unlimited_kafka.packer.StorageManagerParams;

import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class S3ManagerParams implements StorageManagerParams {

    private Long multipartUploadThreshold;
    private Long minimumUploadPartSize;
    private Integer threadPoolSize;
    private String directoryNamePrefix;
    private DateTimeFormatter dateTimeFormatter;

    public void setMultipartUploadThreshold(long multipartUploadThreshold) {
        this.multipartUploadThreshold = multipartUploadThreshold;
    }

    public void setMinimumUploadPartSize(long minimumUploadPartSize) {
        this.minimumUploadPartSize = minimumUploadPartSize;
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

    public Optional<String> getOptionalDirectoryNamePrefix() {
        return Optional.ofNullable(directoryNamePrefix);
    }

    public void setDirectoryNamePrefix(String directoryNamePrefix) {
        this.directoryNamePrefix = directoryNamePrefix;
    }

    public Optional<DateTimeFormatter> getOptionalDateTimeFormatter() {
        return Optional.ofNullable(dateTimeFormatter);
    }

    public void setDateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    public DateTimeFormatter getDateTimeFormatter() {
        return dateTimeFormatter;
    }
}
