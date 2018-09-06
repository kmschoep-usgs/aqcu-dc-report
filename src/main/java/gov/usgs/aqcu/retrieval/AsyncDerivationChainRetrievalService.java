package gov.usgs.aqcu.retrieval;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Processor;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;

/**
 * Serivce which asynchronously makes upchain and downchain processor list requests
 */
@Service
public class AsyncDerivationChainRetrievalService {

	private UpchainProcessorListService upchainProcessorListService;
	private DownchainProcessorListService downchainProcessorListService;

	 @Autowired
	 public AsyncDerivationChainRetrievalService(
		UpchainProcessorListService upchainProcessorListService,
		DownchainProcessorListService downchainProcessorListService) {
				this.upchainProcessorListService = upchainProcessorListService;
				this.downchainProcessorListService = downchainProcessorListService;
	 }
	 
	 @Async("derivationChainRetrievalExecutor")
	 public CompletableFuture<List<Processor>> getAsyncUpchainProcessorListByTimeSeriesUniqueId(String tsUid) {
		  List<Processor> results = upchainProcessorListService.getRawResponse(tsUid, null, null).getProcessors();
		  return CompletableFuture.completedFuture(results); 
	 }

	 @Async("derivationChainRetrievalExecutor")
	 public CompletableFuture<List<Processor>> getAsyncDownchainProcessorListByTimeSeriesUniqueId(String tsUid) {
		  List<Processor> results = downchainProcessorListService.getRawResponse(tsUid, null, null).getProcessors();
		  return CompletableFuture.completedFuture(results); 
	 }
}