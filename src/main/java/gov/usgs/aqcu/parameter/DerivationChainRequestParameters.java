package gov.usgs.aqcu.parameter;

import java.util.ArrayList;
import java.util.List;

public class DerivationChainRequestParameters extends ReportRequestParameters {

	public DerivationChainRequestParameters() {
		
	}

	@Override 
	public String getAsQueryString(String overrideIdentifier, boolean absoluteTime) {
		return super.getAsQueryString(overrideIdentifier, absoluteTime);
	}
}
