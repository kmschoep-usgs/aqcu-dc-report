package gov.usgs.aqcu.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import gov.usgs.aqcu.model.DerivationChainReport;
import gov.usgs.aqcu.model.DerivationChainReportMetadata;
import gov.usgs.aqcu.model.DerivationNode;
import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;
import gov.usgs.aqcu.retrieval.AquariusRetrievalService;
import gov.usgs.aqcu.retrieval.DownchainProcessorListServiceTest;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListServiceTest;
import gov.usgs.aqcu.retrieval.UpchainProcessorListServiceTest;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.LocationDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Processor;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
public class DerivationChainReportBuilderTest {	 
	@MockBean
	private AquariusRetrievalService aquariusService;
	@MockBean
	private DerivationChainBuilderService chainBuilderService;
	@MockBean
	private LocationDescriptionListService locService;
	@MockBean
	private TimeSeriesDescriptionListService descService;

	private DerivationChainReportBuilderService service;
	private final String REQUESTING_USER = "test-user";
	private DerivationChainRequestParameters requestParams;

	DerivationChainReportMetadata metadata;
	TimeSeriesDescription primaryDesc = TimeSeriesDescriptionListServiceTest.DESC_1;
	TimeSeriesDescription procDesc = TimeSeriesDescriptionListServiceTest.DESC_2;
	LocationDescription primaryLoc = new LocationDescription().setIdentifier(primaryDesc.getLocationIdentifier()).setName("loc-name");
	List<Processor> upProcs = UpchainProcessorListServiceTest.PROCESSOR_LIST;
	List<Processor> downProcs = DownchainProcessorListServiceTest.PROCESSOR_LIST;
	DerivationNode nodeA = new DerivationNode(UpchainProcessorListServiceTest.PROC_A, procDesc, new HashSet<>());
	DerivationNode nodeB = new DerivationNode(UpchainProcessorListServiceTest.PROC_B, procDesc, new HashSet<>());
	DerivationNode nodeC = new DerivationNode(DownchainProcessorListServiceTest.PROC_A, procDesc, new HashSet<>());
	DerivationNode nodeD = new DerivationNode(DownchainProcessorListServiceTest.PROC_B, procDesc, new HashSet<>());
	List<DerivationNode> nodes = Arrays.asList(nodeA, nodeB, nodeC, nodeD);

	@Before
	public void setup() {
		 uilder Servies
		service = new DerivationChainReportBuilderService(locService,descService,chainBuilderService);

		// Request Parameters
		requestParams = new DerivationChainRequestParameters();
		requestParams.setPrimaryTimeseriesIdentifier(primaryDesc.getUniqueId());

		// Metadata
		metadata = new DerivationChainReportMetadata();
		metadata.setPrimaryTsIdentifier(requestParams.getPrimaryTimeseriesIdentifier());
		metadata.setStationId(primaryDesc.getLocationIdentifier());
		metadata.setStationName(primaryLoc.getName());
		metadata.setTimezone(primaryDesc.getUtcOffset());
		metadata.setTitle(DerivationChainReportBuilderService.REPORT_TITLE);
	}
	
	@Test
	public void buildReportBasicTest() {
		given(descService.getTimeSeriesDescription(any(String.class)))
			.willReturn(primaryDesc);
		given(locService.getByLocationIdentifier(metadata.getStationId()))
			.willReturn(primaryLoc);
		given(chainBuilderService.buildDerivationChain(any(String.class), any(String.class))).willReturn(nodes);
		
		DerivationChainReport report = service.buildReport(requestParams, REQUESTING_USER);
		assertNotNull(report);
		assertNotNull(report.getReportMetadata());
		assertEquals(report.getReportMetadata().getRequestingUser(), REQUESTING_USER);
		assertEquals(report.getReportMetadata().getStartDate(), metadata.getStartDate());
		assertEquals(report.getReportMetadata().getEndDate(), metadata.getEndDate());
		assertEquals(report.getReportMetadata().getStationId(), primaryDesc.getLocationIdentifier());
		assertEquals(report.getReportMetadata().getTimezone(), metadata.getTimezone());
		assertEquals(report.getReportMetadata().getStationName(), metadata.getStationName());
		assertEquals(report.getReportMetadata().getQualifierMetadata(), new HashMap<>());
		assertEquals(report.getDerivationsInChain(), nodes);
	}

	@Test
	public void getReportMetadataTest() {
		given(locService.getByLocationIdentifier(metadata.getStationId()))
			.willReturn(primaryLoc);

		DerivationChainReportMetadata newMetadata = service.getReportMetadata(requestParams, REQUESTING_USER, primaryLoc.getIdentifier(), primaryDesc.getIdentifier(), primaryDesc.getUtcOffset());
		assertNotNull(newMetadata);
		assertEquals(newMetadata.getRequestingUser(), REQUESTING_USER);
		assertEquals(newMetadata.getStartDate(), metadata.getStartDate());
		assertEquals(newMetadata.getEndDate(), metadata.getEndDate());
		assertEquals(newMetadata.getStationId(), primaryDesc.getLocationIdentifier());
		assertEquals(newMetadata.getStationName(), primaryLoc.getName());
		assertEquals(newMetadata.getTimezone(), metadata.getTimezone());
		assertEquals(newMetadata.getQualifierMetadata(), new HashMap<>());
	}
}
