package gov.usgs.aqcu.model;

import java.util.List;
import java.util.ArrayList;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.RatingCurve;

public class DerivationChainReport {	
	private TimeSeriesDescription primaryTsMetadata;
	private DerivationChainReportMetadata reportMetadata;
	
	
	public DerivationChainReport() {
		primaryTsMetadata = new TimeSeriesDescription();
		reportMetadata = new DerivationChainReportMetadata();
	}
	
	public DerivationChainReportMetadata getReportMetadata() {
		return reportMetadata;
	}
	
	public TimeSeriesDescription getPrimaryTsMetadata() {
		return primaryTsMetadata;
	}
	
	public void setReportMetadata(DerivationChainReportMetadata val) {
		reportMetadata = val;
	}
	
	public void setPrimaryTsMetadata(TimeSeriesDescription val) {
		primaryTsMetadata = val;
	}
}
	
