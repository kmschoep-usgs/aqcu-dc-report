package gov.usgs.aqcu.builder;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import gov.usgs.aqcu.model.DerivationChainReport;
import gov.usgs.aqcu.model.DerivationChainReportMetadata;
import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;

@Service
public class DerivationChainReportBuilderService {
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

	public DerivationChainReport buildReport(DerivationChainRequestParameters requestParameters, String requestingUser) {
		DerivationChainReport report = new DerivationChainReport();

		// Primary TS Metadata
		TimeSeriesDescription primaryDescription = timeSeriesDescriptionListService.getTimeSeriesDescription(requestParameters.getPrimaryTimeseriesIdentifier());

		// Report Metadata
		report.setReportMetadata(getReportMetadata(requestParameters,
			requestingUser,
			primaryDescription.getLocationIdentifier(), 
			primaryDescription.getIdentifier(),
			primaryDescription.getUtcOffset()
		));

		// Build Derivation Chain
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