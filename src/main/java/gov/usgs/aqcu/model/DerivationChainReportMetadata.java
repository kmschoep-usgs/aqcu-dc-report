package gov.usgs.aqcu.model;

public class DerivationChainReportMetadata extends ReportMetadata {
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
	
	public void setPrimaryTsIdentifier(String val) {
		primaryTsIdentifier = val;
	}

	public void setPrimarySeriesLabel(String val) {
		primarySeriesLabel = val;
	}

	public void setRequestingUser(String val) {
		requestingUser = val;
	}
}