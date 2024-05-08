package io.oferto.pocminios3select.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.FileHeaderInfo;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import io.oferto.pocminios3select.config.ObjectStorageConfig;
import io.oferto.pocminios3select.dto.ExpressionRequestDto;
import io.oferto.pocminios3select.dto.CaseRequestDto;
import io.oferto.pocminios3select.model.Expression;
import io.oferto.pocminios3select.model.ProjectionDual;
import io.oferto.pocminios3select.model.ProjectionPrimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnnotationService {
	private final ObjectStorageConfig objectStorageConfig;
	
    private SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, boolean isGzip, String query) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);
                
        InputSerialization inputSerialization = new InputSerialization();
        CSVInput cSVInput = new CSVInput();
        cSVInput.setFileHeaderInfo(FileHeaderInfo.USE);        
        inputSerialization.setCsv(cSVInput);        
        
        if (isGzip)
        	inputSerialization.setCompressionType(CompressionType.GZIP);
        else 
        	inputSerialization.setCompressionType(CompressionType.NONE);
        
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(new CSVOutput());
        
        request.setOutputSerialization(outputSerialization);

        return request;
    }	
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> List<T> convertToExpression(InputStream file, Class<T> responseType) {
        List<T> models;
        try (Reader reader = new BufferedReader(new InputStreamReader(file))) {           
			CsvToBean<?> csvToBean = new CsvToBeanBuilder(reader)
                    .withType(responseType)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build();
            models = (List<T>) csvToBean.parse();
        } catch (Exception ex) {
            log.error("error parsing csv file {} ", ex);
            throw new IllegalArgumentException(ex.getCause().getMessage());
        }
        
        return models;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> List<T> convertToProjection(InputStream file, Class<T> responseType) {
        List<T> models;
        try (Reader reader = new BufferedReader(new InputStreamReader(file))) {           
			CsvToBean<?> csvToBean = new CsvToBeanBuilder(reader)
                    .withType(responseType)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build();
            models = (List<T>) csvToBean.parse();
        } catch (Exception ex) {
            log.error("error parsing csv file {} ", ex);
            throw new IllegalArgumentException(ex.getCause().getMessage());
        }
        
        return models;
    }
    
	public List<Expression> findAllExpressionsByAnnotation(ExpressionRequestDto expressionRequestDto) throws Exception {
		List<Expression> expressions = new ArrayList<Expression>();
		 
		log.debug("findAllExpressionsByAnnotation: found expressions from annotation Id: {}", expressionRequestDto.getAnnotationId());
		
		long start = System.currentTimeMillis();
		NumberFormat formatter = new DecimalFormat("#0.00000");
		
		// disable cert validation
		System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, objectStorageConfig.getDisableTls().toString());
				
		ClientConfiguration clientConfig = new ClientConfiguration(); 
		clientConfig.setProtocol(Protocol.HTTPS);
				
		AWSCredentials credentials = new BasicAWSCredentials(objectStorageConfig.getAccessKey(), objectStorageConfig.getSecretKey());
		
		final AmazonS3 s3Client = AmazonS3ClientBuilder
				.standard()
    		   		.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(objectStorageConfig.getHost() + ":" + objectStorageConfig.getPort(), Regions.US_EAST_1.name()))	    		   		
    		   		.withPathStyleAccessEnabled(true)	    		   		
    		   		.withClientConfiguration(clientConfig)
    		   		.withCredentials(new AWSStaticCredentialsProvider(credentials))	    		   		
    		   .build();
				
		// request annotation expressions
		String query = "select s._1, s.\"" + expressionRequestDto.getAnnotationId() + "\" from s3object s";
		
		boolean isGzip = false;
		if (expressionRequestDto.getKeyObjectName().contains(".gz"))
			isGzip = true;
		
        SelectObjectContentRequest request = generateBaseCSVRequest(expressionRequestDto.getBucketName(), expressionRequestDto.getKeyObjectName(), isGzip, query);
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);
                      
        try (SelectObjectContentResult result = s3Client.selectObjectContent(request)) {        	
        	InputStream resultInputStream = result.getPayload().getRecordsInputStream(
	    		new SelectObjectContentEventVisitor() {
	    			int totBytes = 0;
	    			
	    			@Override
	                public void visit(SelectObjectContentEvent.StatsEvent event) {
	    				System.out.println(
	    						"Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
	    						+  " Bytes Processed: " + event.getDetails().getBytesProcessed());
	    				
	    				System.out.println(
	    						"Received Stats, Bytes Returned: " + event.getDetails().getBytesReturned());	    				
	                }
	
	    			@Override
	                public void visit(SelectObjectContentEvent.RecordsEvent event) {
	    				totBytes = totBytes + event.getPayload().array().length;
	    				    					    				
	    				System.out.println(
	    						"Received RecordsEvent, Bytes Scanned: " + totBytes);
	                }
	    				    			
	                /*
	                 * An End Event informs that the request has finished successfully.
	                 */
	                @Override
	                public void visit(SelectObjectContentEvent.EndEvent event) {	                		                	
	                	isResultComplete.set(true);
	                    System.out.println("Received End Event. Result is complete.");
	                }
	            }
	        );
        	                  	
            // parsing byte array result to entities collection        	
            start = System.currentTimeMillis();
            expressions = convertToExpression(resultInputStream, Expression.class);
            long end = System.currentTimeMillis();
            
            System.out.print("Execution Persist time is " + formatter.format((end - start) / 1000d) + " seconds");
        }
                    
        /*
         * The End Event indicates all matching records have been transmitted.
         * If the End Event is not received, the results may be incomplete.
         */
        if (!isResultComplete.get()) {
            throw new Exception("S3 Select request was incomplete as End Event was not received.");
        }
        
		return expressions;
	}
	
	public List<ProjectionPrimal> findAllPrimalProjectionsBySpace(CaseRequestDto caseRequestDto) throws Exception {
		List<ProjectionPrimal> projectionsPrimal = new ArrayList<ProjectionPrimal>();
		 		
		log.debug("findAllPrimalProjectionsBySpace: found projections with space Id: {}", "primal");
		
		long start = System.currentTimeMillis();
		NumberFormat formatter = new DecimalFormat("#0.00000");
		
		// disable cert validation
		System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, objectStorageConfig.getDisableTls().toString());
				
		ClientConfiguration clientConfig = new ClientConfiguration(); 
		clientConfig.setProtocol(Protocol.HTTPS);
				
		AWSCredentials credentials = new BasicAWSCredentials(objectStorageConfig.getAccessKey(), objectStorageConfig.getSecretKey());
		
		final AmazonS3 s3Client = AmazonS3ClientBuilder
				.standard()
    		   		.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(objectStorageConfig.getHost() + ":" + objectStorageConfig.getPort(), Regions.US_EAST_1.name()))	    		   		
    		   		.withPathStyleAccessEnabled(true)	    		   		
    		   		.withClientConfiguration(clientConfig)
    		   		.withCredentials(new AWSStaticCredentialsProvider(credentials))	    		   		
    		   .build();	       
	       
		String query = "select s.* from s3object s";
		
		boolean isGzip = false;
		if (caseRequestDto.getKeyObjectName().contains(".gz"))
			isGzip = true;
		
        SelectObjectContentRequest request = generateBaseCSVRequest(caseRequestDto.getBucketName(), caseRequestDto.getKeyObjectName(), isGzip, query);
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);

        try (OutputStream fileOutputStream = new FileOutputStream(new File ("/home/miguel/temp/result.csv"));
        		SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
        	InputStream resultInputStream = result.getPayload().getRecordsInputStream(
	    		new SelectObjectContentEventVisitor() {
	    			@Override
	                public void visit(SelectObjectContentEvent.StatsEvent event) {
	    				System.out.println(
	    						"Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
	    						+  " Bytes Processed: " + event.getDetails().getBytesProcessed());
	                }
	
	                /*
	                 * An End Event informs that the request has finished successfully.
	                 */
	                @Override
	                public void visit(SelectObjectContentEvent.EndEvent event) {
	                	isResultComplete.set(true);
	                    System.out.println("Received End Event. Result is complete.");
	                }
	            }
	        );
        	
            // query finalize and show timing
        	long end = System.currentTimeMillis();           
            System.out.print("Execution time is " + formatter.format((end - start) / 1000d) + " seconds");
            
            // parsing result to disk and show timing
            start = System.currentTimeMillis();
            projectionsPrimal = convertToProjection(resultInputStream, ProjectionPrimal.class);
            
            end = System.currentTimeMillis();
            
            System.out.print("Execution Persist time is " + formatter.format((end - start) / 1000d) + " seconds");
        }
                    
        /*
         * The End Event indicates all matching records have been transmitted.
         * If the End Event is not received, the results may be incomplete.
         */
        if (!isResultComplete.get()) {
            throw new Exception("S3 Select request was incomplete as End Event was not received.");
        }
        
        return projectionsPrimal;
	}	
	
	public List<ProjectionDual> findAllDualProjectionsBySpace(CaseRequestDto caseRequestDto) throws Exception {
		List<ProjectionDual> projectionsDual = new ArrayList<ProjectionDual>();
		
		log.debug("findAllDualProjectionsBySpace: found projections with space Id: {}", "dual");
		
		long start = System.currentTimeMillis();
		NumberFormat formatter = new DecimalFormat("#0.00000");
		
		// disable cert validation
		System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, objectStorageConfig.getDisableTls().toString());
				
		ClientConfiguration clientConfig = new ClientConfiguration(); 
		clientConfig.setProtocol(Protocol.HTTPS);
				
		AWSCredentials credentials = new BasicAWSCredentials(objectStorageConfig.getAccessKey(), objectStorageConfig.getSecretKey());
		
		final AmazonS3 s3Client = AmazonS3ClientBuilder
				.standard()
    		   		.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(objectStorageConfig.getHost() + ":" + objectStorageConfig.getPort(), Regions.US_EAST_1.name()))	    		   		
    		   		.withPathStyleAccessEnabled(true)	    		   		
    		   		.withClientConfiguration(clientConfig)
    		   		.withCredentials(new AWSStaticCredentialsProvider(credentials))	    		   		
    		   .build();	       
	       
		String query = "select s.* from s3object s";
		
		boolean isGzip = false;
		if (caseRequestDto.getKeyObjectName().contains(".gz"))
			isGzip = true;
		
        SelectObjectContentRequest request = generateBaseCSVRequest(caseRequestDto.getBucketName(), caseRequestDto.getKeyObjectName(), isGzip, query);
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);

        try (OutputStream fileOutputStream = new FileOutputStream(new File ("/home/miguel/temp/result.csv"));
        		SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
        	InputStream resultInputStream = result.getPayload().getRecordsInputStream(
	    		new SelectObjectContentEventVisitor() {
	    			@Override
	                public void visit(SelectObjectContentEvent.StatsEvent event) {
	    				System.out.println(
	    						"Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
	    						+  " Bytes Processed: " + event.getDetails().getBytesProcessed());
	                }
	
	    			@Override
	                public void visit(SelectObjectContentEvent.ProgressEvent event) {
	    				System.out.println(
	    						"Received Progress, Bytes Scanned: " + event.getDetails().getBytesScanned()
	    						+  " Bytes Processed: " + event.getDetails().getBytesProcessed());
	                }
	    			
	                /*
	                 * An End Event informs that the request has finished successfully.
	                 */
	                @Override
	                public void visit(SelectObjectContentEvent.EndEvent event) {
	                	isResultComplete.set(true);
	                    System.out.println("Received End Event. Result is complete.");
	                }
	            }
	        );
        	
            // query finalize and show timing
        	long end = System.currentTimeMillis();           
            System.out.print("Execution time is " + formatter.format((end - start) / 1000d) + " seconds");
            
            // parsing result to disk and show timing
            start = System.currentTimeMillis();
            projectionsDual = convertToProjection(resultInputStream, ProjectionDual.class);
            
            end = System.currentTimeMillis();
            
            System.out.print("Execution Persist time is " + formatter.format((end - start) / 1000d) + " seconds");
        }
                    
        /*
         * The End Event indicates all matching records have been transmitted.
         * If the End Event is not received, the results may be incomplete.
         */
        if (!isResultComplete.get()) {
            throw new Exception("S3 Select request was incomplete as End Event was not received.");
        }
        
        return projectionsDual;
	}
}
