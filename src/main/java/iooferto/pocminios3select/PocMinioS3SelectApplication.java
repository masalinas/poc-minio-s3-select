package iooferto.pocminios3select;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
import com.amazonaws.services.s3.model.FileHeaderInfo;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;

import static com.amazonaws.SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY;
import static com.amazonaws.util.IOUtils.copy;

@SpringBootApplication
public class PocMinioS3SelectApplication implements CommandLineRunner {
    private static final String HOST = "localhost:9000";
    private static final String ACCESS_KEY = "gl8rbGORHSpxmg1V";
    private static final String SECRET_KEY = "8WphDMckYqRb29s43SzA4trsV2GgaQRc";
    
	private static final String BUCKET_NAME = "65cd021098d02623c46da92d";
    private static final String CSV_BIG_OBJECT_KEY = "65cd02d9e6ba3947be825ac8/66085488056b08fae55840e5/gen_datamatrix.csv";
    private static final String S3_SELECT_RESULTS_PATH = "/home/miguel/temp/result.csv";
    
    private static final String QUERY_COLUMN = "HIF3A";
    private static final String QUERY = "select s._1, s.\"" + QUERY_COLUMN + "\" from s3object s";
        		
	public static void main(String[] args) {
		SpringApplication.run(PocMinioS3SelectApplication.class, args);
	}

	@Override
    public void run(String... args) throws Exception {		
		long start = System.currentTimeMillis();
		long end = 0;
		NumberFormat formatter = new DecimalFormat("#0.00000");
		
		// disable cert validation
		System.setProperty(DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
		
		ClientConfiguration clientConfig = new ClientConfiguration(); 
		clientConfig.setProtocol(Protocol.HTTPS);
		    
		AWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);  	   
		final AmazonS3 s3Client = AmazonS3ClientBuilder
				.standard()
    		   		.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(HOST, Regions.US_EAST_1.name()))	    		   		
    		   		.withPathStyleAccessEnabled(true)	    		   		
    		   		.withClientConfiguration(clientConfig)
    		   		.withCredentials(new AWSStaticCredentialsProvider(credentials))	    		   		
    		   .build();	       
	       
        SelectObjectContentRequest request = generateBaseCSVRequest(BUCKET_NAME, CSV_BIG_OBJECT_KEY, QUERY);
        final AtomicBoolean isResultComplete = new AtomicBoolean(false);

        try (OutputStream fileOutputStream = new FileOutputStream(new File (S3_SELECT_RESULTS_PATH));
            SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
            InputStream resultInputStream = result.getPayload().getRecordsInputStream(
                    new SelectObjectContentEventVisitor() {
                        @Override
                        public void visit(SelectObjectContentEvent.StatsEvent event)
                        {
                            System.out.println(
                                    "Received Stats, Bytes Scanned: " + event.getDetails().getBytesScanned()
                                            +  " Bytes Processed: " + event.getDetails().getBytesProcessed());
                        }

                        /*
                         * An End Event informs that the request has finished successfully.
                         */
                        @Override
                        public void visit(SelectObjectContentEvent.EndEvent event)
                        {
                            isResultComplete.set(true);
                            System.out.println("Received End Event. Result is complete.");
                        }
                    }
            );

            // query finalize and show timing
            end = System.currentTimeMillis();           
            System.out.print("Execution time is " + formatter.format((end - start) / 1000d) + " seconds");
            
            // copy file to disk and show timing
            start = System.currentTimeMillis();
            copy(resultInputStream, fileOutputStream);
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
	}
	
    private static SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
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
}
