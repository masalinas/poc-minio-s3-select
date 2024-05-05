package io.oferto.pocminios3select.model;

import com.opencsv.bean.CsvBindByPosition;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProjectionDual {
	@CsvBindByPosition(position = 0,required = true)	
    private String attributeId;
	@CsvBindByPosition(position = 1,required = true)
    private String expression_type;
	@CsvBindByPosition(position = 2,required = true)
    private float x_sample;	
	@CsvBindByPosition(position = 3,required = true)
    private float y_sample;
}
