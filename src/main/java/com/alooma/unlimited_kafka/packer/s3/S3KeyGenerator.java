package com.alooma.unlimited_kafka.packer.s3;

import java.io.File;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class S3KeyGenerator {

    private final S3ManagerParams s3ManagerParams;
    private final DateTimeFormatter defaultDateFormatter = DateTimeFormatter.ofPattern("yyyy'/'MM'/'dd'/'HH");

    public S3KeyGenerator(S3ManagerParams s3ManagerParams) {
        this.s3ManagerParams = s3ManagerParams;
    }

    public String generate(String topic, boolean shouldUploadAsGz){
        StringBuilder stringBuilder = new StringBuilder();
        DateTimeFormatter dateTimeFormatter = s3ManagerParams.getOptionalDateTimeFormatter().orElse(defaultDateFormatter);

        s3ManagerParams.getOptionalDirectoryNamePrefix().ifPresent(prefix -> stringBuilder.append(prefix).append(File.separator));
        addDate(stringBuilder, dateTimeFormatter);
        stringBuilder.append(topic).append(File.separator).append(UUID.randomUUID().toString());
        addSuffix(shouldUploadAsGz, stringBuilder);

        return stringBuilder.toString();
    }

    private void addDate(StringBuilder stringBuilder, DateTimeFormatter dateTimeFormatter) {
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        try {
            String format = now.format(dateTimeFormatter);
            stringBuilder.append(format.replaceAll("[^0-9a-zA-Z]", File.separator)).append(File.separator);
        } catch (Exception e){
            stringBuilder.append(now.format(defaultDateFormatter)).append(File.separator);
        }
    }

    private void addSuffix(boolean shouldUploadAsGz, StringBuilder stringBuilder) {
        if (shouldUploadAsGz){
            stringBuilder.append(".gz");
        }
    }
}