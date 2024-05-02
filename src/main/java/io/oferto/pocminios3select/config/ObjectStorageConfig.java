package io.oferto.pocminios3select.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;

@Configuration
@Getter
public class ObjectStorageConfig {
    @Value("${object-storage.host: localhost}")
    String host;

    @Value("${object-storage.port: 9000}")
    String port;
    
    @Value("${object-storage.accessKey: gl8rbGORHSpxmg1V}")
    String accessKey;
    
    @Value("${object-storage.secretKey: 8WphDMckYqRb29s43SzA4trsV2GgaQRc}")
    String secretKey;
    
    @Value("${object-storage.disableTls: yes}")
    Boolean disableTls;
}
