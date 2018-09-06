package gov.usgs.aqcu.parameter;

import javax.validation.constraints.NotNull;

public class DerivationChainRequestParameters {
	@NotNull
	private String primaryTimeseriesIdentifier;

	public String getPrimaryTimeseriesIdentifier() {
		return primaryTimeseriesIdentifier;
	}
	public void setPrimaryTimeseriesIdentifier(String primaryTimeseriesIdentifier) {
		this.primaryTimeseriesIdentifier = primaryTimeseriesIdentifier;
	}
}
