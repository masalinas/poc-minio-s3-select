package io.oferto.pocminios3select.dto;

import lombok.Getter;

@Getter
public class ExpressionRequestDto {
	private String bucketName;
	private String keyObjectName;
	private String annotationId;
}
