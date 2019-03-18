package gov.usgs.aqcu.builder;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.HashSet;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Processor;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeRange;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import gov.usgs.aqcu.exception.AquariusRetrievalException;
import gov.usgs.aqcu.model.DerivationNode;
import gov.usgs.aqcu.retrieval.AsyncDerivationChainRetrievalService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesUniqueIdListService;
import gov.usgs.aqcu.util.LogExecutionTime;

@Service
public class DerivationChainBuilderService {
	private static final Logger LOG = LoggerFactory.getLogger(DerivationChainBuilderService.class);
	public static final Integer MAX_TS_DESC_QUERY_SIZE = 45;

	private TimeSeriesUniqueIdListService timeSeriesUniqueIdListService;
	private TimeSeriesDescriptionListService timeSeriesDescriptionListService;
	private AsyncDerivationChainRetrievalService asyncDerivationChainRetrievalService;

	@Autowired
	public DerivationChainBuilderService(
		TimeSeriesUniqueIdListService timeSeriesUniqueIdListService,
		TimeSeriesDescriptionListService timeSeriesDescriptionListService,
		AsyncDerivationChainRetrievalService asyncDerivationChainRetrievalService) {
		this.timeSeriesUniqueIdListService = timeSeriesUniqueIdListService;
		this.timeSeriesDescriptionListService = timeSeriesDescriptionListService;
		this.asyncDerivationChainRetrievalService = asyncDerivationChainRetrievalService;
	}
	@LogExecutionTime
	public List<DerivationNode> buildDerivationChain(String primaryTimeSeriesUniqueId, String locationIdentifier) {
		// List of TS IDs at this site
		List<String> siteTsList = timeSeriesUniqueIdListService.getTimeSeriesUniqueIdList(locationIdentifier);

		// Derivation Chain Data
		Map<String,List<Processor>> procMap = getRecursiveProcessorMap(primaryTimeSeriesUniqueId, siteTsList);
		Map<String,TimeSeriesDescription> tsDescMap = getTimeSeriesDescriptionMap(new ArrayList<>(procMap.keySet()));
		Map<String,Set<String>> derivedTsMap = buildReverseDerivationMap(procMap);
		
		// Derivation Chain
		return buildNodes(procMap, tsDescMap, derivedTsMap);
	}
	@LogExecutionTime
	protected Map<String, List<Processor>> getRecursiveProcessorMap(String primaryTimeSeriesUniqueId, List<String> siteTsList) {
		Map<String, List<Processor>> procMap = new HashMap<>();
		Set<String> exploredSet = new HashSet<>();
		Stack<String> toExplore = new Stack<>();
		toExplore.push(primaryTimeSeriesUniqueId);

		// If there are TS to explore continue exploring
		while(!toExplore.isEmpty()) {
			List<CompletableFuture<List<Processor>>> upProcFutureList = new ArrayList<>();
			List<CompletableFuture<List<Processor>>> downProcFutureList = new ArrayList<>();

			// Empty out the to-explore stack and create async up/down processor requests for each TS (if not already explored)
			while(!toExplore.isEmpty()) {
				String exploreId = toExplore.pop();

				// Create upchain/downchain requests if we haven't already explored this TS
				if(!exploredSet.contains(exploreId)) {
					exploredSet.add(exploreId);

					// Initialize this TS in our procesor map
					procMap.put(exploreId, new ArrayList<>());

					// Request upchain processors
					upProcFutureList.add(asyncDerivationChainRetrievalService.getAsyncUpchainProcessorListByTimeSeriesUniqueId(exploreId));

					// Only request downchain processors if the TS to explore is from the primary TS' site
					if(siteTsList.contains(exploreId)) {
						downProcFutureList.add(asyncDerivationChainRetrievalService.getAsyncDownchainProcessorListByTimeSeriesUniqueId(exploreId));
					}
				}
			}

			// Handle upchain/downchain responses and re-populate our toExplore stack with their input/output TS
			for(Processor proc : waitForFutures(upProcFutureList)) {
				// Can have multiple processors that output the same TS as long as they have unique time ranges
				if(!listContainsEquivalentProcessor(procMap.get(proc.getOutputTimeSeriesUniqueId()), proc)) {	
					procMap.get(proc.getOutputTimeSeriesUniqueId()).add(proc);
				}
				
				// If this TS is at the same site as our primary TS then add upchain TS to our toExplore list
				if(siteTsList.contains(proc.getOutputTimeSeriesUniqueId())) {
					toExplore.addAll(proc.getInputTimeSeriesUniqueIds());
				}
			}
			toExplore.addAll(waitForFutures(downProcFutureList).stream().map(p -> p.getOutputTimeSeriesUniqueId()).collect(Collectors.toSet()));			
		}

		return procMap;
	}
	@LogExecutionTime
	public Set<Processor> waitForFutures(List<CompletableFuture<List<Processor>>> futureList) {
		CompletableFuture<Void> allFutures = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
		try {
			allFutures.get();
		} catch(InterruptedException i) {
			LOG.error("Failed to retrieve all async-requested processors. Caused by: " + i.getMessage());
			throw new AquariusRetrievalException("Failed to retrieve all async-requested processors. Caused by: " + i.getMessage());
		} catch(ExecutionException e) {
			LOG.error("Failed to retrieve all async-requested processors. Caused by: " + e.getMessage());
			throw new AquariusRetrievalException("Failed to retrieve all async-requested processors. Caused by: " + e.getMessage());
		}
		return futureList.stream().map(f -> f.join()).flatMap(List::stream).collect(Collectors.toSet());
	}

	protected boolean listContainsEquivalentProcessor(List<Processor> procList, Processor procCheck) {
		for(Processor tsProc : procList) {
			if(areTimeRangesEquivalent(tsProc.getProcessorPeriod(), procCheck.getProcessorPeriod()) && tsProc.getOutputTimeSeriesUniqueId().equals(procCheck.getOutputTimeSeriesUniqueId())) {
				return true;
			}
		}
		return false;
	}

	protected boolean areTimeRangesEquivalent(TimeRange t1, TimeRange t2) {
		return (t1.getStartTime().equals(t2.getStartTime()) && t1.getEndTime().equals(t2.getEndTime()));
	}

	protected Map<String, TimeSeriesDescription> getTimeSeriesDescriptionMap(List<String> tsIdList) {
		// According to AQ's API docus this is limited to "roughly" 60 items per request, so need to batch
		List<TimeSeriesDescription> tsDescs = new ArrayList<>();
		Map<String,TimeSeriesDescription> tsDescMap = new HashMap<>();
		int endIndex = 0;

		if(!tsIdList.isEmpty()) {
			do {
				int startIndex = endIndex;
				endIndex += MAX_TS_DESC_QUERY_SIZE;
	
				// Bound indicies
				if(startIndex > tsIdList.size()-1) {
					startIndex = tsIdList.size()-1;
					endIndex = tsIdList.size();
				} else if(endIndex > tsIdList.size()) {
					endIndex = tsIdList.size();
				}
	
				// Do fetch
				LOG.debug("Fetching " + (endIndex - startIndex) + " Time Series Descriptions.\nRemaining to fetch: " + (tsIdList.size()-endIndex));
				tsDescs.addAll(timeSeriesDescriptionListService.getTimeSeriesDescriptionList(tsIdList.subList(startIndex, endIndex)));
			}while(endIndex < tsIdList.size());
	
			// Validate that all Descriptions were recieved
			if(tsIdList.size() != tsDescs.size()) {
				LOG.error("Did not recieve all requested Time Series Descriptions! Requested: " + tsIdList.size() + " | Recieved: " + tsDescs.size());
				throw new AquariusRetrievalException("Did not recieve all requested Time Series Descriptions! Requested: " + tsIdList.size() + " | Recieved: " + tsDescs.size());
			}
	
			// Stream to map indexed by TS UID
			tsDescMap = tsDescs.stream().collect(Collectors.toMap(TimeSeriesDescription::getUniqueId,Function.identity()));
		}
		
		return tsDescMap;
	}
	@LogExecutionTime
	protected Map<String, Set<String>> buildReverseDerivationMap(Map<String, List<Processor>> procMap) {
		Map<String, Set<String>> derivedMap = new HashMap<>();
		for(List<Processor> procList : procMap.values()) {
			for(Processor proc : procList) {
				// Add input time series to the set of time series derived from the current output.
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
	@LogExecutionTime
	protected List<DerivationNode> buildNodes(Map<String,List<Processor>> procMap, Map<String,TimeSeriesDescription> tsDescMap, Map<String,Set<String>> derivedTsMap) {
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
}