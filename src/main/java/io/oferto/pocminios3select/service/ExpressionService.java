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
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;
import static com.amazonaws.util.IOUtils.copy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import io.oferto.pocminios3select.config.ObjectStorageConfig;
import io.oferto.pocminios3select.dto.AnnotationRequestDto;
import io.oferto.pocminios3select.model.Expression;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExpressionService {
	private final ObjectStorageConfig objectStorageConfig;
	    
    private SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(bucket);
        request.setKey(key);
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);
        
        InputSerialization inputSerialization = new InputSerialization();
        CSVInput cSVInput = new CSVInput();
        cSVInput.setFileHeaderInfo(FileHeaderInfo.USE);        
        inputSerialization.setCsv(cSVInput);
        inputSerialization.setCompressionType(CompressionType.NONE);
        
        request.setInputSerialization(inputSerialization);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(new CSVOutput());
        
        request.setOutputSerialization(outputSerialization);

        return request;
    }	
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> List<T> convertToModel(InputStream file, Class<T> responseType) {
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
    
	public List<Expression> findAllByAnnotation(AnnotationRequestDto requestAnnotationDto) throws Exception {
		List<Expression> expressions = new ArrayList<Expression>();
		 
		log.debug("findAllByAnnotation: found expressions with annotation Id: {}", requestAnnotationDto.getAnnotationId());
		
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
	       
		String query = "select s._1, s.\"" + requestAnnotationDto.getAnnotationId() + "\" from s3object s";
		
        SelectObjectContentRequest request = generateBaseCSVRequest(requestAnnotationDto.getBucketName(), requestAnnotationDto.getKeyObjectName(), query);
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
            expressions = convertToModel(resultInputStream, Expression.class);
            end = System.currentTimeMillis();
            
            // copy file to disk and show timing
            /*start = System.currentTimeMillis();
            copy(resultInputStream, fileOutputStream);
            end = System.currentTimeMillis();*/

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
}
