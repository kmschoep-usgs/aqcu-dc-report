package gov.usgs.aqcu.builder;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import gov.usgs.aqcu.model.DerivationChainReport;
import gov.usgs.aqcu.model.DerivationChainReportMetadata;
import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;
import gov.usgs.aqcu.util.LogExecutionTime;

@Service
public class DerivationChainReportBuilderService {
	private Logger log = LoggerFactory.getLogger(DerivationChainReportBuilderService.class);
	public static final String REPORT_TITLE = "Derivation Chain";
	public static final String REPORT_TYPE = "derivationchain";

	private TimeSeriesDescriptionListService timeSeriesDescriptionListService;
	private LocationDescriptionListService locationDescriptionListService;
	private DerivationChainBuilderService derivationChainBuilderService;

	@Autowired
	public DerivationChainReportBuilderService(
		LocationDescriptionListService locationDescriptionListService,
		TimeSeriesDescriptionListService timeSeriesDescriptionListService,
		DerivationChainBuilderService derivationChainBuilderService) {
		this.locationDescriptionListService = locationDescriptionListService;
		this.timeSeriesDescriptionListService = timeSeriesDescriptionListService;
		this.derivationChainBuilderService = derivationChainBuilderService;
	}
	
	@LogExecutionTime
	public DerivationChainReport buildReport(DerivationChainRequestParameters requestParameters, String requestingUser) {
		DerivationChainReport report = new DerivationChainReport();

		// Primary TS Metadata
		log.debug("Get primary time series description");
		TimeSeriesDescription primaryDescription = timeSeriesDescriptionListService.getTimeSeriesDescription(requestParameters.getPrimaryTimeseriesIdentifier());

		// Report Metadata
		log.debug("Set report metadata");
		report.setReportMetadata(getReportMetadata(requestParameters,
			requestingUser,
			primaryDescription.getLocationIdentifier(), 
			primaryDescription.getIdentifier(),
			primaryDescription.getUtcOffset()
		));

		// Build Derivation Chain
		log.debug("Build Derivation Chain");
		report.setDerivationsInChain(derivationChainBuilderService.buildDerivationChain(requestParameters.getPrimaryTimeseriesIdentifier(), primaryDescription.getLocationIdentifier()));

		return report;
	}

	protected DerivationChainReportMetadata getReportMetadata(DerivationChainRequestParameters requestParameters, String requestingUser, String stationId, String primaryParameter, Double utcOffset) {
		DerivationChainReportMetadata metadata = new DerivationChainReportMetadata();
		metadata.setTitle(REPORT_TITLE);
		metadata.setRequestingUser(requestingUser);
		metadata.setPrimaryTsIdentifier(requestParameters.getPrimaryTimeseriesIdentifier());
		metadata.setStationId(stationId);
		metadata.setStationName(locationDescriptionListService.getByLocationIdentifier(stationId).getName());
		metadata.setTimezone(utcOffset);
		metadata.setPrimarySeriesLabel(primaryParameter);
		
		return metadata;
	}
}