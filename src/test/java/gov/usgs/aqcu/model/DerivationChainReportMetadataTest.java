package gov.usgs.aqcu.model;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;;

public class DerivationChainReportMetadataTest {
	DerivationChainRequestParameters params = new DerivationChainRequestParameters();

    @Before
    public void setup() {
		params.setPrimaryTimeseriesIdentifier("primary-id");
    }

    @Test
	public void setRequestParametersTest() {
	   DerivationChainReportMetadata metadata = new DerivationChainReportMetadata();
	   metadata.setRequestParameters(params);

	   assertEquals(metadata.getRequestParameters(), params);
	   assertEquals(metadata.getStartDate(), null);
	   assertEquals(metadata.getEndDate(), null);
	   //assertEquals(metadata.getPrimaryTimeSeriesIdentifier(), params.getPrimaryTimeseriesIdentifier());
	}
}