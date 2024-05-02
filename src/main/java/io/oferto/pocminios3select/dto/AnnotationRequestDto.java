package io.oferto.pocminios3select.dto;

import lombok.Getter;

@Getter
public class AnnotationRequestDto {
	private String bucketName;
	private String keyObjectName;
	private String annotationId;
}
