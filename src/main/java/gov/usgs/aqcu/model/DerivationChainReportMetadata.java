package gov.usgs.aqcu.model;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.GradeMetadata;

import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;

public class DerivationChainReportMetadata extends ReportMetadata {
	private DerivationChainRequestParameters requestParameters;
	private String primaryParameter;
	private String primaryTimeSeriesIdentifier;
	private String requestingUser;

	public DerivationChainReportMetadata() {
		super();
	}

	public String getPrimaryTimeSeriesIdentifier() {
		return primaryTimeSeriesIdentifier;
	}
	
	public String getPrimaryParameter() {
		return primaryParameter;
	}

	public String getRequestingUser() {
		return requestingUser;
	}
	
	public DerivationChainRequestParameters getRequestParameters() {
		return requestParameters;
	}
	
	public void setPrimaryTimeSeriesIdentifier(String val) {
		primaryTimeSeriesIdentifier = val;
	}

	public void setPrimaryParameter(String val) {
		primaryParameter = val;
	}

	public void setRequestingUser(String val) {
		requestingUser = val;
	}
	
	public void setRequestParameters(DerivationChainRequestParameters val) {
		requestParameters = val;
		//Report Period displayed should be exactly as recieved, so get as UTC
		setStartDate(val.getStartInstant(ZoneOffset.UTC));
		setEndDate(val.getEndInstant(ZoneOffset.UTC));
		setPrimaryTimeSeriesIdentifier(val.getPrimaryTimeseriesIdentifier());
	}
}