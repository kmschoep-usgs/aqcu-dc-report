package gov.usgs.aqcu.model;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.GradeMetadata;

import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;

public class DerivationChainReportMetadata extends ReportMetadata {
	private DerivationChainRequestParameters requestParameters;
	private String primarySeriesLabel;
	private String primaryTsIdentifier;
	private String requestingUser;

	public DerivationChainReportMetadata() {
		super();
	}

	public String getPrimaryTsIdentifier() {
		return primaryTsIdentifier;
	}
	
	public String getPrimarySeriesLabel() {
		return primarySeriesLabel;
	}

	public String getRequestingUser() {
		return requestingUser;
	}
	
	public DerivationChainRequestParameters getRequestParameters() {
		return requestParameters;
	}
	
	public void setPrimaryTsIdentifier(String val) {
		primaryTsIdentifier = val;
	}

	public void setPrimarySeriesLabel(String val) {
		primarySeriesLabel = val;
	}

	public void setRequestingUser(String val) {
		requestingUser = val;
	}
	
	public void setRequestParameters(DerivationChainRequestParameters val) {
		//Report Period should be null as it doesn't affect this report
		requestParameters = val;
		setPrimaryTsIdentifier(val.getPrimaryTimeseriesIdentifier());
	}
}