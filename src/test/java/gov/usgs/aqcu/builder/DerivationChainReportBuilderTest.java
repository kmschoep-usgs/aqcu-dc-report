package gov.usgs.aqcu.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import gov.usgs.aqcu.model.DerivationChainReport;
import gov.usgs.aqcu.model.DerivationChainReportMetadata;
import gov.usgs.aqcu.parameter.DerivationChainRequestParameters;
import gov.usgs.aqcu.retrieval.AquariusRetrievalService;
import gov.usgs.aqcu.retrieval.DownchainProcessorListService;
import gov.usgs.aqcu.retrieval.LocationDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListServiceTest;
import gov.usgs.aqcu.retrieval.TimeSeriesUniqueIdListService;
import gov.usgs.aqcu.retrieval.UpchainProcessorListService;
import gov.usgs.aqcu.util.AqcuGsonBuilderFactory;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.LocationDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;
import com.google.gson.Gson;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
public class DerivationChainReportBuilderTest {    
	@MockBean
	private AquariusRetrievalService aquariusService;
	@MockBean
	private DownchainProcessorListService downchainService;
	@MockBean 
	private UpchainProcessorListService upchainService;
	@MockBean
	private LocationDescriptionListService locService;
	@MockBean
	private TimeSeriesDescriptionListService descService;
	@MockBean
	private TimeSeriesUniqueIdListService tsUidService;

	private Gson gson;
	private DerivationChainReportBuilderService service;
	private final String REQUESTING_USER = "test-user";
	private DerivationChainRequestParameters requestParams;

	DerivationChainReportMetadata metadata;
	TimeSeriesDescription primaryDesc = TimeSeriesDescriptionListServiceTest.DESC_1;
	LocationDescription primaryLoc = new LocationDescription().setIdentifier(primaryDesc.getLocationIdentifier()).setName("loc-name");

	@Before
	public void setup() {
		//Builder Servies
		gson = AqcuGsonBuilderFactory.getConfiguredGsonBuilder().create();
		service = new DerivationChainReportBuilderService(tsUidService,locService,descService,upchainService,downchainService);

		//Request Parameters
		requestParams = new DerivationChainRequestParameters();
		requestParams.setStartDate(LocalDate.parse("2017-01-01"));
		requestParams.setEndDate(LocalDate.parse("2017-02-01"));
		requestParams.setPrimaryTimeseriesIdentifier(primaryDesc.getUniqueId());

		//Metadata
		metadata = new DerivationChainReportMetadata();
		//metadata.setPrimaryParameter(primaryDesc.getIdentifier());
		metadata.setRequestParameters(requestParams);
		metadata.setStationId(primaryDesc.getLocationIdentifier());
		metadata.setStationName(primaryLoc.getName());
		metadata.setTimezone(primaryDesc.getUtcOffset());
		metadata.setTitle(DerivationChainReportBuilderService.REPORT_TITLE);
	}
	
	/*
	@Test
	@SuppressWarnings("unchecked")
	public void buildReportBasicTest() {
		given(descService.getTimeSeriesDescription(any(String.class)))
			.willReturn(primaryDesc);
		given(descService.getTimeSeriesDescriptionList(any(List.class)))
			.willReturn(TimeSeriesDescriptionListServiceTest.DESC_LIST);
		given(locService.getByLocationIdentifier(metadata.getStationId()))
			.willReturn(primaryLoc);
		
		DerivationChainReport report = service.buildReport(requestParams, REQUESTING_USER);
		assertTrue(report != null);
		assertTrue(report.getReportMetadata() != null);
		assertEquals(report.getReportMetadata().getRequestingUser(), REQUESTING_USER);
		assertEquals(report.getReportMetadata().getPrimaryTimeSeriesIdentifier(), metadata.getPrimaryTimeSeriesIdentifier());
		assertEquals(report.getReportMetadata().getRequestParameters(), metadata.getRequestParameters());
		assertEquals(report.getReportMetadata().getStartDate(), metadata.getStartDate());
		assertEquals(report.getReportMetadata().getEndDate(), metadata.getEndDate());		
		assertEquals(report.getPrimaryTsMetadata(), primaryDesc);
		assertEquals(report.getReportMetadata().getStationId(), primaryDesc.getLocationIdentifier());
		assertEquals(report.getReportMetadata().getPrimaryParameter(), primaryDesc.getIdentifier());
		assertEquals(report.getReportMetadata().getTimezone(), metadata.getTimezone());
		assertEquals(report.getReportMetadata().getStationName(), metadata.getStationName());
		assertEquals(report.getReportMetadata().getQualifierMetadata(), new HashMap<>());
	}
	*/

	@Test
	public void getReportMetadataTest() {
		given(locService.getByLocationIdentifier(metadata.getStationId()))
			.willReturn(primaryLoc);

		DerivationChainReportMetadata newMetadata = service.getReportMetadata(requestParams, REQUESTING_USER, primaryLoc.getIdentifier(), primaryDesc.getIdentifier(), primaryDesc.getUtcOffset());
		assertTrue(newMetadata != null);
		assertEquals(newMetadata.getRequestingUser(), REQUESTING_USER);
		//assertEquals(newMetadata.getPrimaryTimeSeriesIdentifier(), metadata.getPrimaryTimeSeriesIdentifier());
		assertEquals(newMetadata.getRequestParameters(), metadata.getRequestParameters());
		assertEquals(newMetadata.getStartDate(), metadata.getStartDate());
		assertEquals(newMetadata.getEndDate(), metadata.getEndDate());
		assertEquals(newMetadata.getStationId(), primaryDesc.getLocationIdentifier());
		assertEquals(newMetadata.getStationName(), primaryLoc.getName());
		//assertEquals(newMetadata.getPrimaryParameter(), primaryDesc.getIdentifier());
		assertEquals(newMetadata.getTimezone(), metadata.getTimezone());
		assertEquals(newMetadata.getQualifierMetadata(), new HashMap<>());
	}
}
