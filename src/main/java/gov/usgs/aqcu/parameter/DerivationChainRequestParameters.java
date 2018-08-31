package gov.usgs.aqcu.parameter;

public class DerivationChainRequestParameters extends ReportRequestParameters {

	public DerivationChainRequestParameters() {
		
	}

	@Override 
	public String getAsQueryString(String overrideIdentifier, boolean absoluteTime) {
		return super.getAsQueryString(overrideIdentifier, absoluteTime);
	}
}
