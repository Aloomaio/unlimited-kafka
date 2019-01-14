package com.alooma.unlimited_kafka.packer.s3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class S3KeyGenerator {

    private final S3ManagerParams s3ManagerParams;
    private final DateTimeFormatter defaultDateFormatter = DateTimeFormatter.ofPattern("yyyy'/'MM'/'dd'/'HH");

    public S3KeyGenerator(S3ManagerParams s3ManagerParams) {
        this.s3ManagerParams = s3ManagerParams;
    }


    public String generate(String topic){
        StringBuilder stringBuilder = new StringBuilder();
        s3ManagerParams.getOptionalDirectoryNamePrefix().ifPresent(prefix -> stringBuilder.append(prefix).append("/"));
        DateTimeFormatter dateTimeFormatter = s3ManagerParams.getOptionalDateTimeFormatter().orElse(defaultDateFormatter);

        //todo: check if date should be now or in time zone according to region

        LocalDateTime localDateTime = LocalDateTime.now();
        try {
            String format = localDateTime.format(dateTimeFormatter);
            stringBuilder.append(format.replaceAll("[^0-9a-zA-Z]", "/")).append("/");
        }catch (Exception e){
            stringBuilder.append(localDateTime.format(defaultDateFormatter)).append("/");
        }

        UUID uuid = UUID.randomUUID();
        stringBuilder.append(topic).append("/").append(uuid.toString());

        //todo: add .gz when needed


        return stringBuilder.toString();
    }
}
