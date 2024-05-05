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

import io.oferto.pocminios3select.service.AnnotationService;
import io.oferto.pocminios3select.dto.ExpressionRequestDto;
import io.oferto.pocminios3select.dto.CaseRequestDto;
import io.oferto.pocminios3select.model.Expression;
import io.oferto.pocminios3select.model.ProjectionDual;
import io.oferto.pocminios3select.model.ProjectionPrimal;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("annotation")
public class AnnotationController {
	private final AnnotationService annotationService;
			
	@RequestMapping(value = "/expressions", method = { RequestMethod.GET }, produces = "application/json")
	public ResponseEntity<?> findAllExpressionsByAnnotation(@RequestBody ExpressionRequestDto expressionRequestDto) {
		List<Expression> expressions;
		
		try {
			expressions = (List<Expression>) annotationService.findAllExpressionsByAnnotation(expressionRequestDto);
			
			log.debug("findAllExpressionsByAnnotation: found {} expressions", expressions.size());
			
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
	
	@RequestMapping(value = "/projections/primal",method = { RequestMethod.GET }, produces = "application/json")
	public ResponseEntity<?> findAllPrimalProjections(@RequestBody CaseRequestDto caseRequestDto) {
		List<ProjectionPrimal> projections;
		
		try {
			projections = (List<ProjectionPrimal>) annotationService.findAllPrimalProjectionsBySpace(caseRequestDto);
			
			log.debug("findAllPrimalProjectionsBySpace: found {} projections", projections.size());
			
			return ResponseEntity
		            .status(HttpStatus.OK)
		            .body(projections);
		} catch (Exception e) {
			e.printStackTrace();
			
			return ResponseEntity
		            .status(HttpStatus.INTERNAL_SERVER_ERROR)
		            .body(e.getMessage());
		}			
	}
	
	@RequestMapping(value = "/projections/dual",method = { RequestMethod.GET }, produces = "application/json")
	public ResponseEntity<?> findAllDualProjections(@RequestBody CaseRequestDto caseRequestDto) {
		List<ProjectionDual> projections;
		
		try {
			projections = (List<ProjectionDual>) annotationService.findAllDualProjectionsBySpace(caseRequestDto);
			
			log.debug("findAllDualProjectionsBySpace: found {} projections", projections.size());
			
			return ResponseEntity
		            .status(HttpStatus.OK)
		            .body(projections);
		} catch (Exception e) {
			e.printStackTrace();
			
			return ResponseEntity
		            .status(HttpStatus.INTERNAL_SERVER_ERROR)
		            .body(e.getMessage());
		}			
	}	
}
