package io.oferto.pocminios3select.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import io.oferto.pocminios3select.service.ExpressionService;
import io.oferto.pocminios3select.dto.AnnotationRequestDto;
import io.oferto.pocminios3select.model.Expression;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("expressions")
public class ExpressionController {
	private final ExpressionService expressionService;
			
	@RequestMapping(method = { RequestMethod.GET }, produces = "application/json")
	public ResponseEntity<?> findAllByAnnotation(@RequestBody AnnotationRequestDto requestAnnotationDto) {
		List<Expression> expressions;
		
		try {
			expressions = (List<Expression>) expressionService.findAllByAnnotation(requestAnnotationDto);
			
			log.debug("findAllByAnnotation: found {} expressions", expressions.size());
			
			return ResponseEntity
		            .status(HttpStatus.OK)
		            .body(expressions);
		} catch (Exception e) {
			e.printStackTrace();
			
			return ResponseEntity
		            .status(HttpStatus.INTERNAL_SERVER_ERROR)
		            .body(e.getMessage());
		}			
	}
}
