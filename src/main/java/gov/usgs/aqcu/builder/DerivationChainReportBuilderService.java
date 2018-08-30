package gov.usgs.aqcu.builder;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Processor;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeRange;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import gov.usgs.aqcu.exception.AquariusRetrievalException;
import gov.usgs.aqcu.model.DerivationChainReport;
import gov.usgs.aqcu.model.DerivationChainReportMetadata;
import gov.usgs.aqcu.model.DerivationNode;
import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;
import gov.usgs.aqcu.retrieval.DownchainProcessorListService;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesUniqueIdListService;
import gov.usgs.aqcu.retrieval.UpchainProcessorListService;

@Service
public class DerivationChainReportBuilderService {
	public static final String REPORT_TITLE = "Derivation Chain";
	public static final String REPORT_TYPE = "derivationchain";
	public static final Integer MAX_TS_DESC_QUERY_SIZE = 30;
	private static final Logger LOG = LoggerFactory.getLogger(DerivationChainReportBuilderService.class);

	private TimeSeriesUniqueIdListService timeSeriesUniqueIdListService;
	private LocationDescriptionListService locationDescriptionListService;
	private TimeSeriesDescriptionListService timeSeriesDescriptionListService;
	private UpchainProcessorListService upchainProcessorListService;
	private DownchainProcessorListService downchainProcessorListService;

	@Autowired
	public DerivationChainReportBuilderService(
		TimeSeriesUniqueIdListService timeSeriesUniqueIdListService,
		LocationDescriptionListService locationDescriptionListService,
		TimeSeriesDescriptionListService timeSeriesDescriptionListService,
		UpchainProcessorListService upchainProcessorListService,
		DownchainProcessorListService downchainProcessorListService) {
		this.timeSeriesUniqueIdListService = timeSeriesUniqueIdListService;
		this.locationDescriptionListService = locationDescriptionListService;
		this.timeSeriesDescriptionListService = timeSeriesDescriptionListService;
		this.upchainProcessorListService = upchainProcessorListService;
		this.downchainProcessorListService = downchainProcessorListService;
	}

	public DerivationChainReport buildReport(DerivationChainRequestParameters requestParameters, String requestingUser) {
		DerivationChainReport report = new DerivationChainReport();

		//Primary TS Metadata
		TimeSeriesDescription primaryDescription = timeSeriesDescriptionListService.getTimeSeriesDescription(requestParameters.getPrimaryTimeseriesIdentifier());

		//Report Metadata
		report.setReportMetadata(getReportMetadata(requestParameters,
			requestingUser,
			primaryDescription.getLocationIdentifier(), 
			primaryDescription.getIdentifier(),
			primaryDescription.getUtcOffset()
		));

		//List of TS IDs at this site
		List<String> siteTsList = timeSeriesUniqueIdListService.getTimeSeriesUniqueIdList(primaryDescription.getLocationIdentifier());

		//Fetch Derivation Chain Data
		Map<String,List<Processor>> procMap = getRecursiveProcessorMap(requestParameters.getPrimaryTimeseriesIdentifier(), siteTsList);
		Map<String,TimeSeriesDescription> tsDescMap = getTimeSeriesDesciprionMap(new ArrayList<>(procMap.keySet()));
		Map<String,Set<String>> derivedTsMap = buildReverseDerivationMap(procMap);

		//Build Derivation Nodes
		List<DerivationNode> derivationNodes = buildNodes(procMap, tsDescMap, derivedTsMap);
		report.setDerivationsInChain(derivationNodes);

		return report;
	}

	public Map<String, List<Processor>> getRecursiveProcessorMap(String primaryTimeSeriesUniqueId, List<String> siteTsList) {
		Map<String, List<Processor>> procMap = new HashMap<>();
		Set<String> exploredSet = new HashSet<>();
		Stack<String> toExplore = new Stack<>();
		toExplore.push(primaryTimeSeriesUniqueId);

		// Find all Time Series upchain of the root and all downchain series, that is any other series that our current
		// set of explored series are derived from.
		while(!toExplore.isEmpty()) {
			String root = toExplore.pop();
			if(!exploredSet.contains(root)) {
				exploredSet.add(root);

				// Initialize this TS in the map, if necessary
				if(procMap.get(root) == null) {
					procMap.put(root, new ArrayList<>());
				}

				LOG.debug("Fetching up/down processors for TS: " + root);

				//Find upchain processors and add them to our explored processor map.
				List<Processor> upProcs = upchainProcessorListService.getRawResponse(root, null, null).getProcessors();
				for(Processor proc : upProcs) {
					//Can have multiple processors that output the same TS as long as they have unique time ranges
					if(procMap.get(proc.getOutputTimeSeriesUniqueId()).isEmpty() || 
						!listContainsEquivalentProcessor(procMap.get(proc.getOutputTimeSeriesUniqueId()), proc)
					) {	
						procMap.get(proc.getOutputTimeSeriesUniqueId()).add(proc);
					}
					
					//If this TS is at the same site as our primary TS then add upchain TS to our toExplore list
					if(siteTsList.contains(root)) {
						toExplore.addAll(proc.getInputTimeSeriesUniqueIds());
					}
				}

				//If this TS is at the same site as our primary TS then find downchain processors and add their output TS to our toExplore list.
				if(siteTsList.contains(root)) {
					List<Processor> downProcs = downchainProcessorListService.getRawResponse(root, null, null).getProcessors();
					toExplore.addAll(downchainProcessorListService.getOutputTimeSeriesUniqueIdList(downProcs));
				}
			}
		}

		return procMap;
	}

	/**
	 * Whether or not the provided list of processors contains a processor with the same time range and output.
	 */
	public boolean listContainsEquivalentProcessor(List<Processor> procList, Processor procCheck) {
		for(Processor tsProc : procList) {
			if(areTimeRangesEquivalent(tsProc.getProcessorPeriod(), procCheck.getProcessorPeriod())) {
				return true;
			}
		}
		return false;
	}

	public boolean areTimeRangesEquivalent(TimeRange t1, TimeRange t2) {
		return (t1.getStartTime().equals(t2.getStartTime()) && t1.getEndTime().equals(t2.getEndTime()));
	}

	public Map<String, TimeSeriesDescription> getTimeSeriesDesciprionMap(List<String> tsIdList) {
		//According to AQ's API docus this is limited to "roughly" 60 items per request, so need to batch
		List<TimeSeriesDescription> tsDescs = new ArrayList<>();
		int startIndex = 0;
		int endIndex = 0;

		do {
			startIndex = endIndex;
			endIndex += MAX_TS_DESC_QUERY_SIZE;

			//Bound indicies
			if(startIndex > tsIdList.size()-1) {
				startIndex = tsIdList.size()-1;
				endIndex = tsIdList.size();
			} else if(endIndex > tsIdList.size()) {
				endIndex = tsIdList.size();
			}

			//Do fetch
			LOG.debug("Fetching " + (endIndex - startIndex + 1) + " Time Series Descriptions.\nRemaining to fetch: " + (tsIdList.size()-endIndex));
			tsDescs.addAll(timeSeriesDescriptionListService.getTimeSeriesDescriptionList(tsIdList.subList(startIndex, endIndex)));
		}while(endIndex < tsIdList.size());

		//Validate that all Descriptions were recieved
		if(tsIdList.size() != tsDescs.size()) {
			throw new AquariusRetrievalException("Did not recieve all requested Time Series Descriptions! Requested: " + tsIdList.size() + " | Recieved: " + tsDescs.size());
		}

		//Stream to map indexed by TS UID
		Map<String,TimeSeriesDescription> tsDescMap = tsDescs.stream().collect(Collectors.toMap(TimeSeriesDescription::getUniqueId,Function.identity()));
		
		return tsDescMap;
	}

	/**
	 * For each time series, builds a list of other time series that are derived from it (time-range agnostic).
	 */
	public Map<String, Set<String>> buildReverseDerivationMap(Map<String, List<Processor>> procMap) {
		Map<String, Set<String>> derivedMap = new HashMap<>();
		for(List<Processor> procList : procMap.values()) {
			for(Processor proc : procList) {
				//Add input time series to the set of time series derived from the current output.
				for(String input : proc.getInputTimeSeriesUniqueIds()) {
					if(derivedMap.get(input) == null) {
						derivedMap.put(input, new HashSet<>(Arrays.asList(proc.getOutputTimeSeriesUniqueId())));
					} else {
						derivedMap.get(input).add(proc.getOutputTimeSeriesUniqueId());
					}
				}
				
			}
		}
		return derivedMap;
	}

	public List<DerivationNode> buildNodes(Map<String,List<Processor>> procMap, Map<String,TimeSeriesDescription> tsDescMap, Map<String,Set<String>> derivedTsMap) {
		List<DerivationNode> output = new ArrayList<>();
		for(String tsUid : procMap.keySet()) {
			if(!procMap.get(tsUid).isEmpty()) {
				for(Processor proc : procMap.get(tsUid)) {
					output.add(new DerivationNode(proc, tsDescMap.get(tsUid), derivedTsMap.get(tsUid)));
				}
			} else {
				output.add(new DerivationNode(null, tsDescMap.get(tsUid), derivedTsMap.get(tsUid)));
			}
		}
		return output;
	}

	protected DerivationChainReportMetadata getReportMetadata(DerivationChainRequestParameters requestParameters, String requestingUser, String stationId, String primaryParameter, Double utcOffset) {
		DerivationChainReportMetadata metadata = new DerivationChainReportMetadata();
		metadata.setTitle(REPORT_TITLE);
		metadata.setRequestingUser(requestingUser);
		metadata.setRequestParameters(requestParameters);
		metadata.setStationId(stationId);
		metadata.setStationName(locationDescriptionListService.getByLocationIdentifier(stationId).getName());
		metadata.setTimezone(utcOffset);
		metadata.setPrimarySeriesLabel(primaryParameter);
		
		return metadata;
	}
}