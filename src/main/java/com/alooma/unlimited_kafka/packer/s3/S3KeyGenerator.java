package com.alooma.unlimited_kafka.packer.s3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class S3KeyGenerator {

    private final S3ManagerParams s3ManagerParams;
    private final DateTimeFormatter defaultDateFormatter = DateTimeFormatter.ofPattern("yyyy'/'MM'/'dd'/'HH");

    public S3KeyGenerator(S3ManagerParams s3ManagerParams) {
        this.s3ManagerParams = s3ManagerParams;
    }


    public String genrate(String topic){
        StringBuilder stringBuilder = new StringBuilder();
        s3ManagerParams.getOptionalDirectoryNamePrefix().ifPresent(prefix -> stringBuilder.append(prefix).append("/"));

        DateTimeFormatter dateTimeFormatter = s3ManagerParams.getOptionalDateTimeFormatter().ifPresent();

        //todo: check if date should be now or in time zone accoeding to region

        LocalDateTime localDateTime = LocalDateTime.now();
        try {
            String format = localDateTime.format(dateTimeFormatter);
            stringBuilder.append(format.replaceAll("[^0-9a-zA-Z]", "/")).append("/");
        }catch (Exception e){
            stringBuilder.append(localDateTime.format(defaultDateFormatter)).append("/");
        }





        //String key = String.format("%s/%d", topic, offset).concat(".gz");

        return stringBuilder.toString();
    }




}
