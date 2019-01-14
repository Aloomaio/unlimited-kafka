package com.alooma.unlimited_kafka.packer.s3;

import org.junit.jupiter.api.Test;

import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class S3KeyGeneratorTest {


    private final Pattern defaultPattern = Pattern.compile(".*\\d{4}/\\d{2}/\\d{2}/\\d{2}/.*/[a-z0-9\\-]{36}");
    private final Pattern pattern = Pattern.compile(".*\\d{2}/\\w{3}/\\d{2}/\\d{2}/.*/[a-z0-9\\-]{36}");

    @Test
    void testKeyGenerator() {

        String topic = "test-topic";
        S3ManagerParams s3ManagerParams = new S3ManagerParamsBuilder()
                .withDirectoryNamePrefix("noa")
                .withDirectoryNameDateTimeFormatter(DateTimeFormatter.ofPattern("yy'/'MMM'/'D'/'HH"))
                .build();
        S3KeyGenerator keyGenerator = new S3KeyGenerator(s3ManagerParams);

        String generatedKey = keyGenerator.genrate(topic);

        assertTrue(pattern.matcher(generatedKey).matches());

    }

    @Test
    void testKeyGenerator_defaultDateTimeFormat() {

        String topic = "test-topic";
        S3ManagerParams s3ManagerParams = new S3ManagerParamsBuilder()
                .withDirectoryNamePrefix("noa")
                .build();
        S3KeyGenerator keyGenerator = new S3KeyGenerator(s3ManagerParams);

        String generatedKey = keyGenerator.genrate(topic);

        assertTrue(defaultPattern.matcher(generatedKey).matches());

    }

    @Test
    void testKeyGenerator_withoutPrefix() {

        String topic = "test-topic";
        S3ManagerParams s3ManagerParams = new S3ManagerParamsBuilder()
                .build();
        S3KeyGenerator keyGenerator = new S3KeyGenerator(s3ManagerParams);

        String generatedKey = keyGenerator.genrate(topic);

        assertTrue(defaultPattern.matcher(generatedKey).matches());

    }

    //todo: test gz


}