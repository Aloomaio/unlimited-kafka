package com.alooma.unlimited_kafka.packer.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class S3KeyGenerator {

    private final S3ManagerParams s3ManagerParams;
    private final DateTimeFormatter defaultDateFormatter = DateTimeFormatter.ofPattern("yyyy'/'MM'/'dd'/'HH");

    private static final Logger logger = LogManager.getLogger("MessagePackerS3");

    public S3KeyGenerator(S3ManagerParams s3ManagerParams) {
        this.s3ManagerParams = s3ManagerParams;
    }

    public String generate(String topic) {
        StringBuilder stringBuilder = new StringBuilder();
        s3ManagerParams.getOptionalDirectoryNamePrefix().ifPresent(prefix -> stringBuilder.append(prefix).append(File.separator));
        DateTimeFormatter dateTimeFormatter = s3ManagerParams.getOptionalDateTimeFormatter().orElse(defaultDateFormatter);
        addDate(stringBuilder, dateTimeFormatter);
        stringBuilder.append(topic).append(File.separator).append(UUID.randomUUID().toString());
        addSuffix(stringBuilder);

        return stringBuilder.toString();
    }

    protected void addDate(StringBuilder stringBuilder, DateTimeFormatter dateTimeFormatter) {
        ZonedDateTime utcTime = ZonedDateTime.now(ZoneOffset.UTC);
        try {
            String format = utcTime.format(dateTimeFormatter);
            stringBuilder.append(format.replaceAll("[^0-9a-zA-Z]", File.separator)).append(File.separator);
        } catch (Exception e) {
            logger.debug("Key generator Failed to format date, using default format.");
            stringBuilder.append(utcTime.format(defaultDateFormatter)).append(File.separator);
        }
    }

    private void addSuffix(StringBuilder stringBuilder) {
        if (s3ManagerParams.isShouldUploadAsGzip()) {
            stringBuilder.append(".gz");
        }
    }
}
