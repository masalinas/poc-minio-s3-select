package io.oferto.pocminios3select.model;

import com.opencsv.bean.CsvBindByPosition;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProjectionPrimal {
	@CsvBindByPosition(position = 0,required = true)	
    private String sampleId;
	@CsvBindByPosition(position = 1,required = false)
    private String cancer_code;
	@CsvBindByPosition(position = 2,required = false)
    private String cancer_description;
	@CsvBindByPosition(position = 3,required = false)
    private float age_at_diagnosis;
	@CsvBindByPosition(position = 4,required = false)
    private String tumor_stage;
	@CsvBindByPosition(position = 5,required = false)
    private String gender;	
	@CsvBindByPosition(position = 6,required = false)
    private String race;
	@CsvBindByPosition(position = 7,required = false)
    private String vital_status;
	@CsvBindByPosition(position = 8,required = false)
    private float age_at_index;
	@CsvBindByPosition(position = 9,required = false)
    private String morphology;
	@CsvBindByPosition(position = 10,required = false)
    private String primary_diagnosis;
	@CsvBindByPosition(position = 11,required = false)
    private String site_of_resection_or_biopsy;
	@CsvBindByPosition(position = 12,required = false)
    private String ajcc_pathologic_m;
	@CsvBindByPosition(position = 13,required = false)
    private String ajcc_pathologic_n;
	@CsvBindByPosition(position = 14,required = false)
    private String ajcc_pathologic_stage;
	@CsvBindByPosition(position = 15,required = false)
    private String ajcc_pathologic_t;
	@CsvBindByPosition(position = 16,required = false)
    private String classification_of_tumor;
	@CsvBindByPosition(position = 17,required = false)
    private String metastasis_at_diagnosis;	
	@CsvBindByPosition(position = 18,required = false)
    private String prior_malignancy;	
	@CsvBindByPosition(position = 19,required = false)
    private float year_of_diagnosis;	
	@CsvBindByPosition(position = 20,required = false)
    private String treatment_type;	
	@CsvBindByPosition(position = 21,required = false)
    private String ethnicity;	
	@CsvBindByPosition(position = 22,required = true)
    private float x_rna;	
	@CsvBindByPosition(position = 23,required = true)
    private float y_rna;	
	@CsvBindByPosition(position = 24,required = false)
    private String case_id;	
}
