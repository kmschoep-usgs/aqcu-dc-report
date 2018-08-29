package gov.usgs.aqcu.builder;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

import java.util.HashMap;
import java.time.Instant;
import java.time.ZoneOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import gov.usgs.aqcu.model.DerivationChainReport;
import gov.usgs.aqcu.model.DerivationChainReportMetadata;
import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;
import gov.usgs.aqcu.util.TimeSeriesUtils;

@Service
public class DerivationChainReportBuilderService {
	public static final String REPORT_TITLE = "Derivation Chain";
	public static final String REPORT_TYPE = "derivationchain";
	private static final Logger LOG = LoggerFactory.getLogger(DerivationChainReportBuilderService.class);

	private LocationDescriptionListService locationDescriptionListService;
	private TimeSeriesDescriptionListService timeSeriesDescriptionListService;

	@Autowired
	public DerivationChainReportBuilderService(
		LocationDescriptionListService locationDescriptionListService,
		TimeSeriesDescriptionListService timeSeriesDescriptionListService) {
		this.locationDescriptionListService = locationDescriptionListService;
		this.timeSeriesDescriptionListService = timeSeriesDescriptionListService;		
	}

	public DerivationChainReport buildReport(DerivationChainRequestParameters requestParameters, String requestingUser) {
		DerivationChainReport report = new DerivationChainReport();

		//Primary TS Metadata
		TimeSeriesDescription primaryDescription = timeSeriesDescriptionListService.getTimeSeriesDescription(requestParameters.getPrimaryTimeseriesIdentifier());
		ZoneOffset primaryZoneOffset = TimeSeriesUtils.getZoneOffset(primaryDescription);
		String primaryStationId = primaryDescription.getLocationIdentifier();
		report.setPrimaryTsMetadata(primaryDescription);

		//Report Metadata
		report.setReportMetadata(getReportMetadata(requestParameters,
			requestingUser,
			report.getPrimaryTsMetadata().getLocationIdentifier(), 
			report.getPrimaryTsMetadata().getIdentifier(),
			report.getPrimaryTsMetadata().getUtcOffset()
		));

		return report;
	}

	protected DerivationChainReportMetadata getReportMetadata(DerivationChainRequestParameters requestParameters, String requestingUser, String stationId, String primaryParameter, Double utcOffset) {
		DerivationChainReportMetadata metadata = new DerivationChainReportMetadata();
		metadata.setTitle(REPORT_TITLE);
		metadata.setRequestingUser(requestingUser);
		metadata.setRequestParameters(requestParameters);
		metadata.setStationId(stationId);
		metadata.setStationName(locationDescriptionListService.getByLocationIdentifier(stationId).getName());
		metadata.setTimezone(utcOffset);
		metadata.setPrimaryParameter(primaryParameter);
		
		return metadata;
	}
}