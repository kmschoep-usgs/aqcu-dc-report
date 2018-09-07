package gov.usgs.aqcu.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.when;
import static org.mockito.Matchers.any;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import gov.usgs.aqcu.exception.AquariusRetrievalException;
import gov.usgs.aqcu.model.DerivationNode;
import gov.usgs.aqcu.retrieval.AsyncDerivationChainRetrievalService;
import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListService;
import gov.usgs.aqcu.retrieval.TimeSeriesUniqueIdListService;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Processor;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeRange;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

@RunWith(SpringRunner.class)
@ActiveProfiles("test")
public class DerivationChainBuilderTest {
	@MockBean
	private TimeSeriesUniqueIdListService tsUidsService;
	@MockBean
	private TimeSeriesDescriptionListService descService;
	@MockBean
	private AsyncDerivationChainRetrievalService asyncService;

	private DerivationChainBuilderService service;

	CompletableFuture<List<Processor>> mockEmptyFuture = new CompletableFuture<>();
	List<String> fullIdList = Arrays.asList("R", "D1", "D2", "U1", "U2", "U3", "UU1", "UU2", "UD", "DU", "DD", "UUU", "UUD1", "UUD2", "UDU", "UDD", "DUU", "DUD", "DDU", "DDD");
	List<TimeSeriesDescription> fullDescList;

	Processor procRC = new Processor();
	Processor procU1C = new Processor();
	Processor procU2A = new Processor();
	Processor procU2B = new Processor();
	Processor procU3B = new Processor();
	Processor procD1A = new Processor();
	Processor procD1B = new Processor();
	Processor procD2A = new Processor();
	Processor procD2B = new Processor();
	Processor procUU1A = new Processor();
	Processor procUU2B = new Processor();
	Processor procUDA = new Processor();
	Processor procUDB = new Processor();
	Processor procDUA = new Processor();
	Processor procDDA = new Processor();
	Processor procDDB = new Processor();
	Processor procUUD1A = new Processor();
	Processor procUUD2B = new Processor();
	Processor procUDDA = new Processor();
	Processor procDUDA = new Processor();
	Processor procDDDC = new Processor();

	TimeSeriesDescription descR = new TimeSeriesDescription();
	TimeSeriesDescription descU1 = new TimeSeriesDescription();
	TimeSeriesDescription descU2 = new TimeSeriesDescription();
	TimeSeriesDescription descU3 = new TimeSeriesDescription();
	TimeSeriesDescription descD1 = new TimeSeriesDescription();
	TimeSeriesDescription descD2 = new TimeSeriesDescription();
	TimeSeriesDescription descUU1 = new TimeSeriesDescription();
	TimeSeriesDescription descUU2 = new TimeSeriesDescription();
	TimeSeriesDescription descUD = new TimeSeriesDescription();
	TimeSeriesDescription descDU = new TimeSeriesDescription();
	TimeSeriesDescription descDD = new TimeSeriesDescription();
	TimeSeriesDescription descUUU = new TimeSeriesDescription();
	TimeSeriesDescription descUUD1 = new TimeSeriesDescription();
	TimeSeriesDescription descUUD2 = new TimeSeriesDescription();
	TimeSeriesDescription descUDU = new TimeSeriesDescription();
	TimeSeriesDescription descUDD = new TimeSeriesDescription();
	TimeSeriesDescription descDUU = new TimeSeriesDescription();
	TimeSeriesDescription descDUD = new TimeSeriesDescription();
	TimeSeriesDescription descDDU = new TimeSeriesDescription();
	TimeSeriesDescription descDDD = new TimeSeriesDescription();

	TimeRange procPeriodA;
	TimeRange procPeriodB;
	TimeRange procPeriodC;

	@Before
	public void setup() {
		// Builder Service
		service = new DerivationChainBuilderService(tsUidsService, descService, asyncService);

		mockEmptyFuture.obtrudeValue(new ArrayList<>());

		// Build test chain time ranges
		procPeriodA = new TimeRange();
		procPeriodA.setStartTime(Instant.parse("2016-01-01T00:00:00Z"));
		procPeriodA.setEndTime(Instant.parse("2017-01-01T00:00:00Z"));
		procPeriodB = new TimeRange();
		procPeriodB.setStartTime(Instant.parse("2017-01-01T00:00:00Z"));
		procPeriodB.setEndTime(Instant.parse("2018-01-01T00:00:00Z"));
		procPeriodC = new TimeRange();
		procPeriodC.setStartTime(Instant.parse("2016-01-01T00:00:00Z"));
		procPeriodC.setEndTime(Instant.parse("2018-01-01T00:00:00Z"));

		// Build test chain Processors
		procRC.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("U1", "U2", "U3")));
		procRC.setOutputTimeSeriesUniqueId("R");
		procRC.setProcessorPeriod(procPeriodC);
		procU1C.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU1")));
		procU1C.setOutputTimeSeriesUniqueId("U1");
		procU1C.setProcessorPeriod(procPeriodC);
		procU2A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU1")));
		procU2A.setOutputTimeSeriesUniqueId("U2");
		procU2A.setProcessorPeriod(procPeriodA);
		procU2B.setInputTimeSeriesUniqueIds(new ArrayList<>());
		procU2B.setOutputTimeSeriesUniqueId("U2");
		procU2B.setProcessorPeriod(procPeriodB);
		procU3B.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU2")));
		procU3B.setOutputTimeSeriesUniqueId("U3");
		procU3B.setProcessorPeriod(procPeriodB);
		procD1A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("R", "DU")));
		procD1A.setOutputTimeSeriesUniqueId("D1");
		procD1A.setProcessorPeriod(procPeriodA);
		procD1B.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("R")));
		procD1B.setOutputTimeSeriesUniqueId("D1");
		procD1B.setProcessorPeriod(procPeriodB);
		procD2A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("R")));
		procD2A.setOutputTimeSeriesUniqueId("D2");
		procD2A.setProcessorPeriod(procPeriodA);
		procD2B.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("R")));
		procD2B.setOutputTimeSeriesUniqueId("D2");
		procD2B.setProcessorPeriod(procPeriodB);
		procUU1A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UUU")));
		procUU1A.setOutputTimeSeriesUniqueId("UU1");
		procUU1A.setProcessorPeriod(procPeriodA);
		procUU2B.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UUU")));
		procUU2B.setOutputTimeSeriesUniqueId("UU2");
		procUU2B.setProcessorPeriod(procPeriodA);
		procUDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("U2", "UDU")));
		procUDA.setOutputTimeSeriesUniqueId("UD");
		procUDA.setProcessorPeriod(procPeriodA);
		procUDB.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("U2")));
		procUDB.setOutputTimeSeriesUniqueId("UD");
		procUDB.setProcessorPeriod(procPeriodB);
		procDUA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("DUU")));
		procDUA.setOutputTimeSeriesUniqueId("DU");
		procDUA.setProcessorPeriod(procPeriodA);
		procDDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("D1", "D2", "DDU")));
		procDDA.setOutputTimeSeriesUniqueId("DD");
		procDDA.setProcessorPeriod(procPeriodA);
		procDDB.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("D1", "D2")));
		procDDB.setOutputTimeSeriesUniqueId("DD");
		procDDB.setProcessorPeriod(procPeriodB);
		procUUD1A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU1")));
		procUUD1A.setOutputTimeSeriesUniqueId("UUD1");
		procUUD1A.setProcessorPeriod(procPeriodA);
		procUUD2B.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU2")));
		procUUD2B.setOutputTimeSeriesUniqueId("UUD2");
		procUUD2B.setProcessorPeriod(procPeriodB);
		procUDDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UD")));
		procUDDA.setOutputTimeSeriesUniqueId("UDD");
		procUDDA.setProcessorPeriod(procPeriodA);
		procDUDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("DU")));
		procDUDA.setOutputTimeSeriesUniqueId("DUD");
		procDUDA.setProcessorPeriod(procPeriodA);
		procDDDC.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("DD")));
		procDDDC.setOutputTimeSeriesUniqueId("DDD");
		procDDDC.setProcessorPeriod(procPeriodA);

		// Build test chain Descriptions
		descR.setUniqueId("R");
		descU1.setUniqueId("U1");
		descU2.setUniqueId("U2");
		descU3.setUniqueId("U3");
		descD1.setUniqueId("D1");
		descD2.setUniqueId("D2");
		descUU1.setUniqueId("UU1");
		descUU2.setUniqueId("UU2");
		descUD.setUniqueId("UD");
		descDU.setUniqueId("DU");
		descDD.setUniqueId("DD");
		descUUU.setUniqueId("UUU");
		descUUD1.setUniqueId("UUD1");
		descUUD2.setUniqueId("UUD2");
		descUDU.setUniqueId("UDU");
		descUDD.setUniqueId("UDD");
		descDUU.setUniqueId("DUU");
		descDUD.setUniqueId("DUD");
		descDDU.setUniqueId("DDU");
		descDDD.setUniqueId("DDD");
		fullDescList = Arrays.asList(descR, descU1, descU2, descU3, descD1, descD2, descUU1, descUU2, descUD, descDU, descDD, descUUU, descUUD1, descUUD2, descUDU, descUDD, descDUU, descDUD, descDDU, descDDD);

		setupProcResponses();
	}

	public void setupProcResponses() {
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockAsyncProcs(Arrays.asList(procRC)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockAsyncProcs(Arrays.asList(procD1A, procD2A, procD1B, procD2B)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("U1")).willReturn(mockAsyncProcs(Arrays.asList(procU1C)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("U1")).willReturn(mockAsyncProcs(Arrays.asList(procRC)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("U2")).willReturn(mockAsyncProcs(Arrays.asList(procU2A, procU2B)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("U2")).willReturn(mockAsyncProcs(Arrays.asList(procRC, procUDA, procUDB)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("U3")).willReturn(mockAsyncProcs(Arrays.asList(procU3B)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("U3")).willReturn(mockAsyncProcs(Arrays.asList(procRC)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("D1")).willReturn(mockAsyncProcs(Arrays.asList(procD1A, procD1B)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("D1")).willReturn(mockAsyncProcs(Arrays.asList(procDDA, procDDB)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("D2")).willReturn(mockAsyncProcs(Arrays.asList(procD2A, procD2B)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("D2")).willReturn(mockAsyncProcs(Arrays.asList(procDDA, procDDB)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UU1")).willReturn(mockAsyncProcs(Arrays.asList(procUU1A)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UU1")).willReturn(mockAsyncProcs(Arrays.asList(procU1C, procU2A, procUUD1A)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UU2")).willReturn(mockAsyncProcs(Arrays.asList(procUU2B)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UU2")).willReturn(mockAsyncProcs(Arrays.asList(procU3B, procUUD2B)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UD")).willReturn(mockAsyncProcs(Arrays.asList(procUDA, procUDB)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UD")).willReturn(mockAsyncProcs(Arrays.asList(procUDDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DU")).willReturn(mockAsyncProcs(Arrays.asList(procDUA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DU")).willReturn(mockAsyncProcs(Arrays.asList(procDUDA, procD1A)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DD")).willReturn(mockAsyncProcs(Arrays.asList(procDDA, procDDB)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DD")).willReturn(mockAsyncProcs(Arrays.asList(procDDDC)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UUU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UUU")).willReturn(mockAsyncProcs(Arrays.asList(procUU1A, procUU2B)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UUD1")).willReturn(mockAsyncProcs(Arrays.asList(procUUD1A)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UUD1")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UUD2")).willReturn(mockAsyncProcs(Arrays.asList(procUUD2B)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UUD2")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UDU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UDU")).willReturn(mockAsyncProcs(Arrays.asList(procUDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UDD")).willReturn(mockAsyncProcs(Arrays.asList(procUDDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UDD")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DUU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DUU")).willReturn(mockAsyncProcs(Arrays.asList(procDUA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DUD")).willReturn(mockAsyncProcs(Arrays.asList(procDUDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DUD")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DDU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DDU")).willReturn(mockAsyncProcs(Arrays.asList(procDDA, procDDB)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DDD")).willReturn(mockAsyncProcs(Arrays.asList(procDDDC)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DDD")).willReturn(mockEmptyFuture);
	}

	@Test
	public void waitForFuturesTest() {
		List<CompletableFuture<List<Processor>>> futureList = new ArrayList<>();
		futureList.add(mockAsyncProcs(Arrays.asList(procRC)));
		futureList.add(mockAsyncProcs(Arrays.asList(procD1A, procD2A, procD1B, procD2B)));
		futureList.add(mockAsyncProcs(Arrays.asList(procU2A, procU2B)));
		Set<Processor> result = service.waitForFutures(futureList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertThat(result, containsInAnyOrder(procRC, procU2A, procU2B, procD1A, procD2A, procD1B, procD2B));
	}

	@Test
	public void waitForFuturesEmptyResponseTest() {
		List<CompletableFuture<List<Processor>>> futureList = new ArrayList<>();
		futureList.add(mockEmptyFuture);
		futureList.add(mockAsyncProcs(Arrays.asList(procD1A, procD2A, procD1B, procD2B)));
		Set<Processor> result = service.waitForFutures(futureList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertThat(result, containsInAnyOrder(procD1A, procD2A, procD1B, procD2B));
	}

	@Test
	public void waitForFuturesEmptyTest() {
		List<CompletableFuture<List<Processor>>> futureList = new ArrayList<>();
		Set<Processor> result = service.waitForFutures(futureList);
		assertNotNull(result);
		assertTrue(result.isEmpty());
	}

	@Test
	public void getRecursiveProcessorMapNoUpTest() {
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockAsyncProcs(Arrays.asList(procD1A, procD2A, procD1B, procD2B)));
		Map<String, List<Processor>> result = service.getRecursiveProcessorMap("R", fullIdList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(9, result.keySet().size());
		assertEquals(9, result.values().size());
		assertThat(result.keySet(), containsInAnyOrder("R", "D1", "D2", "DU", "DD", "DUU", "DUD", "DDU", "DDD"));
		assertNotNull(result.get("R"));
		assertTrue(result.get("R").isEmpty());
		assertNull(result.get("U1"));
		assertNull(result.get("U2"));
		assertNull(result.get("U3"));
		assertNotNull(result.get("D1"));
		assertThat(result.get("D1"), containsInAnyOrder(procD1A, procD1B));
		assertNotNull(result.get("D2"));
		assertThat(result.get("D2"), containsInAnyOrder(procD2A, procD2B));
		assertNull(result.get("UU1"));
		assertNull(result.get("UU2"));
		assertNull(result.get("UD"));
		assertNotNull(result.get("DU"));
		assertThat(result.get("DU"), containsInAnyOrder(procDUA));
		assertNotNull(result.get("DD"));
		assertThat(result.get("DD"), containsInAnyOrder(procDDA, procDDB));
		assertNull(result.get("UUU"));
		assertNull(result.get("UUD1"));
		assertNull(result.get("UUD2"));
		assertNull(result.get("UDU"));
		assertNull(result.get("UDD"));
		assertNotNull(result.get("DUU"));
		assertTrue(result.get("DUU").isEmpty());
		assertNotNull(result.get("DUD"));
		assertThat(result.get("DUD"), containsInAnyOrder(procDUDA));
		assertNotNull(result.get("DDU"));
		assertTrue(result.get("DDU").isEmpty());
		assertNotNull(result.get("DDD"));
		assertThat(result.get("DDD"), containsInAnyOrder(procDDDC));
	}

	@Test
	public void getRecursiveProcessorMapNoDownTest() {
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockAsyncProcs(Arrays.asList(procRC)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockEmptyFuture);
		Map<String, List<Processor>> result = service.getRecursiveProcessorMap("R", fullIdList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(12, result.keySet().size());
		assertEquals(12, result.values().size());
		assertThat(result.keySet(), containsInAnyOrder("R", "U1", "U2", "U3", "UU1", "UU2", "UD", "UUU", "UUD1", "UUD2", "UDU", "UDD"));
		assertNotNull(result.get("R"));
		assertThat(result.get("R"), containsInAnyOrder(procRC));
		assertNotNull(result.get("U1"));
		assertThat(result.get("U1"), containsInAnyOrder(procU1C));
		assertNotNull(result.get("U2"));
		assertThat(result.get("U2"), containsInAnyOrder(procU2A, procU2B));
		assertNotNull(result.get("U3"));
		assertThat(result.get("U3"), containsInAnyOrder(procU3B));
		assertNull(result.get("D1"));
		assertNull(result.get("D2"));
		assertNotNull(result.get("UU1"));
		assertThat(result.get("UU1"), containsInAnyOrder(procUU1A));
		assertNotNull(result.get("UU2"));
		assertThat(result.get("UU2"), containsInAnyOrder(procUU2B));
		assertNotNull(result.get("UD"));
		assertThat(result.get("UD"), containsInAnyOrder(procUDA, procUDB));
		assertNull(result.get("DU"));
		assertNull(result.get("DD"));
		assertNotNull(result.get("UUU"));
		assertTrue(result.get("UUU").isEmpty());
		assertNotNull(result.get("UUD1"));
		assertThat(result.get("UUD1"), containsInAnyOrder(procUUD1A));
		assertNotNull(result.get("UUD2"));
		assertThat(result.get("UUD2"), containsInAnyOrder(procUUD2B));
		assertNotNull(result.get("UDU"));
		assertTrue(result.get("UDU").isEmpty());
		assertNotNull(result.get("UDD"));
		assertThat(result.get("UDD"), containsInAnyOrder(procUDDA));
		assertNull(result.get("DUU"));
		assertNull(result.get("DUD"));
		assertNull(result.get("DDU"));
		assertNull(result.get("DDD"));
		
	}

	@Test
	public void getRecursiveProcessorMapNoUpOrDownTest() {
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockEmptyFuture);
		Map<String, List<Processor>> result = service.getRecursiveProcessorMap("R", fullIdList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(1, result.keySet().size());
		assertEquals(1, result.values().size());
		assertThat(result.keySet(), containsInAnyOrder("R"));
		assertNotNull(result.get("R"));
		assertTrue(result.get("R").isEmpty());
		assertNull(result.get("U1"));
		assertNull(result.get("U2"));
		assertNull(result.get("U3"));
		assertNull(result.get("D1"));
		assertNull(result.get("D2"));
		assertNull(result.get("UU1"));
		assertNull(result.get("UU2"));
		assertNull(result.get("UD"));
		assertNull(result.get("DU"));
		assertNull(result.get("DD"));
		assertNull(result.get("UUU"));
		assertNull(result.get("UUD1"));
		assertNull(result.get("UUD2"));
		assertNull(result.get("UDU"));
		assertNull(result.get("UDD"));
		assertNull(result.get("DUU"));
		assertNull(result.get("DUD"));
		assertNull(result.get("DDU"));
		assertNull(result.get("DDD"));
	}

	@Test
	public void getRecursiveProcessorMapInclusiveTest() {
		Map<String, List<Processor>> result = service.getRecursiveProcessorMap("R", fullIdList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(20, result.keySet().size());
		assertEquals(20, result.values().size());
		assertThat(result.keySet(), containsInAnyOrder(fullIdList.toArray()));
		assertNotNull(result.get("R"));
		assertThat(result.get("R"), containsInAnyOrder(procRC));
		assertNotNull(result.get("U1"));
		assertThat(result.get("U1"), containsInAnyOrder(procU1C));
		assertNotNull(result.get("U2"));
		assertThat(result.get("U2"), containsInAnyOrder(procU2A, procU2B));
		assertNotNull(result.get("U3"));
		assertThat(result.get("U3"), containsInAnyOrder(procU3B));
		assertNotNull(result.get("D1"));
		assertThat(result.get("D1"), containsInAnyOrder(procD1A, procD1B));
		assertNotNull(result.get("D2"));
		assertThat(result.get("D2"), containsInAnyOrder(procD2A, procD2B));
		assertNotNull(result.get("UU1"));
		assertThat(result.get("UU1"), containsInAnyOrder(procUU1A));
		assertNotNull(result.get("UU2"));
		assertThat(result.get("UU2"), containsInAnyOrder(procUU2B));
		assertNotNull(result.get("UD"));
		assertThat(result.get("UD"), containsInAnyOrder(procUDA, procUDB));
		assertNotNull(result.get("DU"));
		assertThat(result.get("DU"), containsInAnyOrder(procDUA));
		assertNotNull(result.get("DD"));
		assertThat(result.get("DD"), containsInAnyOrder(procDDA, procDDB));
		assertNotNull(result.get("UUU"));
		assertTrue(result.get("UUU").isEmpty());
		assertNotNull(result.get("UUD1"));
		assertThat(result.get("UUD1"), containsInAnyOrder(procUUD1A));
		assertNotNull(result.get("UUD2"));
		assertThat(result.get("UUD2"), containsInAnyOrder(procUUD2B));
		assertNotNull(result.get("UDU"));
		assertTrue(result.get("UDU").isEmpty());
		assertNotNull(result.get("UDD"));
		assertThat(result.get("UDD"), containsInAnyOrder(procUDDA));
		assertNotNull(result.get("DUU"));
		assertTrue(result.get("DUU").isEmpty());
		assertNotNull(result.get("DUD"));
		assertThat(result.get("DUD"), containsInAnyOrder(procDUDA));
		assertNotNull(result.get("DDU"));
		assertTrue(result.get("DDU").isEmpty());
		assertNotNull(result.get("DDD"));
		assertThat(result.get("DDD"), containsInAnyOrder(procDDDC));
	}

	@Test
	public void getRecursiveProcessorMapLimitedTest() {
		Map<String, List<Processor>> result = service.getRecursiveProcessorMap("R", Arrays.asList("R", "U1", "U2", "D1", "D2", "UU1", "DD", "UUU", "DDD"));
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(15, result.keySet().size());
		assertEquals(15, result.values().size());
		assertThat(result.keySet(), containsInAnyOrder("R", "U1", "U2", "U3", "D1", "D2", "UU1", "UU2", "UD", "DU", "DD", "UUU", "UUD1", "DDU", "DDD"));
		assertNotNull(result.get("R"));
		assertThat(result.get("R"), containsInAnyOrder(procRC));
		assertNotNull(result.get("U1"));
		assertThat(result.get("U1"), containsInAnyOrder(procU1C));
		assertNotNull(result.get("U2"));
		assertThat(result.get("U2"), containsInAnyOrder(procU2A, procU2B));
		assertNotNull(result.get("U3"));
		assertThat(result.get("U3"), containsInAnyOrder(procU3B));
		assertNotNull(result.get("D1"));
		assertThat(result.get("D1"), containsInAnyOrder(procD1A, procD1B));
		assertNotNull(result.get("D2"));
		assertThat(result.get("D2"), containsInAnyOrder(procD2A, procD2B));
		assertNotNull(result.get("UU1"));
		assertThat(result.get("UU1"), containsInAnyOrder(procUU1A));
		assertNotNull(result.get("UU2"));
		assertThat(result.get("UU2"), containsInAnyOrder(procUU2B));
		assertNotNull(result.get("UD"));
		assertThat(result.get("UD"), containsInAnyOrder(procUDA, procUDB));
		assertNotNull(result.get("DU"));
		assertThat(result.get("DU"), containsInAnyOrder(procDUA));
		assertNotNull(result.get("DD"));
		assertThat(result.get("DD"), containsInAnyOrder(procDDA, procDDB));
		assertNotNull(result.get("UUU"));
		assertTrue(result.get("UUU").isEmpty());
		assertNotNull(result.get("UUD1"));
		assertThat(result.get("UUD1"), containsInAnyOrder(procUUD1A));
		assertNull(result.get("UUD2"));
		assertNull(result.get("UDU"));
		assertNull(result.get("UDD"));
		assertNull(result.get("DUU"));
		assertNull(result.get("DUD"));
		assertNotNull(result.get("DDU"));
		assertNotNull(result.get("DDU"));
		assertTrue(result.get("DDU").isEmpty());
		assertNotNull(result.get("DDD"));
		assertThat(result.get("DDD"), containsInAnyOrder(procDDDC));
	}

	@Test
	public void listContainsEquivalentProcessorTest() {
		List<Processor> procList1 = Arrays.asList(procD1A, procD1B, procD2A);
		Processor sameRangeAndOutputDifferentInputD1A = new Processor();
		Processor sameRangeAndOutputDifferentInputD1B = new Processor();
		Processor sameRangeAndOutputDifferentInputD2A = new Processor();
		Processor sameRangeAndOutputDifferentInputD2B = new Processor();
		Processor sameRangeDifferentOutputD1A = new Processor();
		Processor sameRangeDifferentOutputD1B = new Processor();
		Processor sameRangeDifferentOutputD2A = new Processor();
		Processor sameRangeDifferentOutputD2B = new Processor();
		Processor differentRangeSameOutputD1AB = new Processor();
		Processor differentRangeSameOutputD2AB = new Processor();
		sameRangeAndOutputDifferentInputD1A.setProcessorPeriod(procPeriodA);
		sameRangeAndOutputDifferentInputD1A.setOutputTimeSeriesUniqueId("D1");
		sameRangeAndOutputDifferentInputD1A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UUU", "UU1", "UU2")));
		sameRangeAndOutputDifferentInputD1B.setProcessorPeriod(procPeriodB);
		sameRangeAndOutputDifferentInputD1B.setOutputTimeSeriesUniqueId("D1");
		sameRangeAndOutputDifferentInputD1B.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UUU", "UU1", "UU2")));
		sameRangeAndOutputDifferentInputD2A.setProcessorPeriod(procPeriodA);
		sameRangeAndOutputDifferentInputD2A.setOutputTimeSeriesUniqueId("D2");
		sameRangeAndOutputDifferentInputD2A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UUU", "UU1", "UU2")));
		sameRangeAndOutputDifferentInputD2B.setProcessorPeriod(procPeriodB);
		sameRangeAndOutputDifferentInputD2B.setOutputTimeSeriesUniqueId("D2");
		sameRangeAndOutputDifferentInputD2B.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UUU", "UU1", "UU2")));
		sameRangeDifferentOutputD1A.setProcessorPeriod(procPeriodA);
		sameRangeDifferentOutputD1A.setOutputTimeSeriesUniqueId("diff");
		sameRangeDifferentOutputD1B.setProcessorPeriod(procPeriodA);
		sameRangeDifferentOutputD1B.setOutputTimeSeriesUniqueId("diff");
		sameRangeDifferentOutputD2A.setProcessorPeriod(procPeriodA);
		sameRangeDifferentOutputD2A.setOutputTimeSeriesUniqueId("diff");
		sameRangeDifferentOutputD2B.setProcessorPeriod(procPeriodA);
		sameRangeDifferentOutputD2B.setOutputTimeSeriesUniqueId("diff");
		differentRangeSameOutputD1AB.setProcessorPeriod(procPeriodC);
		differentRangeSameOutputD1AB.setOutputTimeSeriesUniqueId("D1");
		differentRangeSameOutputD2AB.setProcessorPeriod(procPeriodC);
		differentRangeSameOutputD2AB.setOutputTimeSeriesUniqueId("D1");
		assertTrue(service.listContainsEquivalentProcessor(procList1, procD1A));
		assertTrue(service.listContainsEquivalentProcessor(procList1, procD1B));
		assertTrue(service.listContainsEquivalentProcessor(procList1, procD2A));
		assertFalse(service.listContainsEquivalentProcessor(procList1, procD2B));
		assertTrue(service.listContainsEquivalentProcessor(procList1, sameRangeAndOutputDifferentInputD1A));
		assertTrue(service.listContainsEquivalentProcessor(procList1, sameRangeAndOutputDifferentInputD1B));
		assertTrue(service.listContainsEquivalentProcessor(procList1, sameRangeAndOutputDifferentInputD2A));
		assertFalse(service.listContainsEquivalentProcessor(procList1, sameRangeAndOutputDifferentInputD2B));
		assertFalse(service.listContainsEquivalentProcessor(procList1, sameRangeDifferentOutputD1A));
		assertFalse(service.listContainsEquivalentProcessor(procList1, sameRangeDifferentOutputD1B));
		assertFalse(service.listContainsEquivalentProcessor(procList1, sameRangeDifferentOutputD2A));
		assertFalse(service.listContainsEquivalentProcessor(procList1, sameRangeDifferentOutputD2B));
		assertFalse(service.listContainsEquivalentProcessor(procList1, differentRangeSameOutputD1AB));
		assertFalse(service.listContainsEquivalentProcessor(procList1, differentRangeSameOutputD2AB));
	}

	@Test
	public void areTimeRangesEquivalentTest() {
		assertTrue(service.areTimeRangesEquivalent(procPeriodA, procPeriodA));
		assertTrue(service.areTimeRangesEquivalent(procPeriodB, procPeriodB));
		assertTrue(service.areTimeRangesEquivalent(procPeriodC, procPeriodC));
		assertFalse(service.areTimeRangesEquivalent(procPeriodA, procPeriodB));
		assertFalse(service.areTimeRangesEquivalent(procPeriodA, procPeriodC));
		assertFalse(service.areTimeRangesEquivalent(procPeriodB, procPeriodC));
	}

	@Test
	public void getTimeSeriesDescriptionMapSingleTest() {
		given(descService.getTimeSeriesDescriptionList(Arrays.asList("R"))).willReturn(Arrays.asList(descR));
		Map<String, TimeSeriesDescription> result = service.getTimeSeriesDescriptionMap(Arrays.asList("R"));
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(result.size(), 1);
		assertEquals(result.get("R"), descR);
	}

	@Test
	public void getTimeSeriesDescriptionMapSuccessTest() {
		given(descService.getTimeSeriesDescriptionList(fullIdList)).willReturn(fullDescList);
		Map<String, TimeSeriesDescription> result = service.getTimeSeriesDescriptionMap(fullIdList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(result.size(), 20);
		assertEquals(result.get("R"), descR);
		assertEquals(result.get("U1"), descU1);
		assertEquals(result.get("U2"), descU2);
		assertEquals(result.get("U3"), descU3);
		assertEquals(result.get("D1"), descD1);
		assertEquals(result.get("D2"), descD2);
		assertEquals(result.get("UU1"), descUU1);
		assertEquals(result.get("UU2"), descUU2);
		assertEquals(result.get("UD"), descUD);
		assertEquals(result.get("DU"), descDU);
		assertEquals(result.get("DD"), descDD);
		assertEquals(result.get("UUU"), descUUU);
		assertEquals(result.get("UUD1"), descUUD1);
		assertEquals(result.get("UUD2"), descUUD2);
		assertEquals(result.get("UDU"), descUDU);
		assertEquals(result.get("UDD"), descUDD);
		assertEquals(result.get("DUU"), descDUU);
		assertEquals(result.get("DUD"), descDUD);
		assertEquals(result.get("DDU"), descDDU);
		assertEquals(result.get("DDD"), descDDD);
	}

	@Test
	public void getTimeSeriesDescriptionMapFailTest() {
		given(descService.getTimeSeriesDescriptionList(fullIdList)).willReturn(fullDescList.subList(1, fullDescList.size()-2));
		try {
			service.getTimeSeriesDescriptionMap(fullIdList);
		} catch(AquariusRetrievalException ARE) {
			assertTrue(ARE.getMessage().contains("Did not recieve all requested Time Series Descriptions!"));
			return;
		} catch(Exception e) {
			fail("Expected AquariusRetrievalException but " + e.getClass().toString() + " was thrown.");
		}
		fail("Expected AquariusRetrievalException but no exception was thrown.");
	}

	@Test
	public void getTimeSeriesDescriptionMapBatchingTest() {
		// Build Mock ID List
		List<String> mockIdList = new ArrayList<>();
		for(Integer i = 0; i < (DerivationChainBuilderService.MAX_TS_DESC_QUERY_SIZE*2 + 1); i++) {
			mockIdList.add(i.toString());
		}

		// Build Expected Requests
		List<String> request1 = mockIdList.subList(0, DerivationChainBuilderService.MAX_TS_DESC_QUERY_SIZE);
		List<String> request2 = mockIdList.subList(DerivationChainBuilderService.MAX_TS_DESC_QUERY_SIZE, DerivationChainBuilderService.MAX_TS_DESC_QUERY_SIZE*2);
		List<String> request3 = mockIdList.subList(DerivationChainBuilderService.MAX_TS_DESC_QUERY_SIZE*2, mockIdList.size());

		// Build Expected Responses
		List<TimeSeriesDescription> response1 = new ArrayList<>();
		for(String id : request1) {
			TimeSeriesDescription desc = new TimeSeriesDescription();
			desc.setUniqueId(id);
			response1.add(desc);
		}
		List<TimeSeriesDescription> response2 = new ArrayList<>();
		for(String id : request2) {
			TimeSeriesDescription desc = new TimeSeriesDescription();
			desc.setUniqueId(id);
			response1.add(desc);
		}
		List<TimeSeriesDescription> response3 = new ArrayList<>();
		for(String id : request3) {
			TimeSeriesDescription desc = new TimeSeriesDescription();
			desc.setUniqueId(id);
			response1.add(desc);
		}
		List<TimeSeriesDescription> fullResults = new ArrayList<>();
		fullResults.addAll(response1);
		fullResults.addAll(response2);
		fullResults.addAll(response3);

		// Mock Expected Requests/Responses
		given(descService.getTimeSeriesDescriptionList(request1)).willReturn(response1);
		given(descService.getTimeSeriesDescriptionList(request2)).willReturn(response2);
		given(descService.getTimeSeriesDescriptionList(request3)).willReturn(response3);

		// Execute Test
		Map<String, TimeSeriesDescription> result = service.getTimeSeriesDescriptionMap(mockIdList);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(result.size(), (DerivationChainBuilderService.MAX_TS_DESC_QUERY_SIZE*2 + 1));
		assertThat(result.keySet(), containsInAnyOrder(mockIdList.toArray()));
		assertThat(result.values(), containsInAnyOrder(fullResults.toArray()));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void getTimeSeriesDescriptionMapEmptyTest() {
		given(descService.getTimeSeriesDescriptionList(any(ArrayList.class))).willReturn(Arrays.asList(descR));
		Map<String, TimeSeriesDescription> result = service.getTimeSeriesDescriptionMap(new ArrayList<>());
		assertNotNull(result);
		assertTrue(result.isEmpty());
	}

	@Test
	public void buildReverseDerivationMapTest() {
		Map<String, List<Processor>> procMap = new HashMap<>();
		procMap.put("R", Arrays.asList(procRC));
		procMap.put("U1", Arrays.asList(procU1C));
		procMap.put("U2", Arrays.asList(procU2A, procU2B));
		procMap.put("U3", Arrays.asList(procU3B));
		procMap.put("D1", Arrays.asList(procD1A, procD1B));
		procMap.put("D2", Arrays.asList(procD2A, procD2B));
		procMap.put("DDD", Arrays.asList(procDDDC));
		procMap.put("UUU", new ArrayList<>());
		Map<String, Set<String>> result = service.buildReverseDerivationMap(procMap);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(result.size(), 8);
		assertThat(result.keySet(), containsInAnyOrder("R", "U1", "U2", "U3", "UU1", "UU2", "DU", "DD"));
		assertThat(result.get("R"), containsInAnyOrder("D1", "D2"));
		assertThat(result.get("U1"), containsInAnyOrder("R"));
		assertThat(result.get("U2"), containsInAnyOrder("R"));
		assertThat(result.get("U3"), containsInAnyOrder("R"));
		assertThat(result.get("UU1"), containsInAnyOrder("U1", "U2"));
		assertThat(result.get("UU2"), containsInAnyOrder("U3"));
		assertThat(result.get("DU"), containsInAnyOrder("D1"));
		assertThat(result.get("DD"), containsInAnyOrder("DDD"));
	}

	@Test
	public void buildNodesEmptyTest() {
		List<DerivationNode> result = service.buildNodes(new HashMap<>(), new HashMap<>(), new HashMap<>());
		assertNotNull(result);
		assertTrue(result.isEmpty());
	}

	@Test
	public void buildNodesFullTest() {
		Map<String, List<Processor>> procMap = new HashMap<>();
		procMap.put("R", Arrays.asList(procRC));
		procMap.put("U1", Arrays.asList(procU1C));
		procMap.put("U2", Arrays.asList(procU2A, procU2B));
		procMap.put("U3", Arrays.asList(procU3B));
		procMap.put("D1", Arrays.asList(procD1A, procD1B));
		procMap.put("D2", Arrays.asList(procD2A, procD2B));
		procMap.put("DDD", Arrays.asList(procDDDC));
		procMap.put("UUU", new ArrayList<>());

		Map<String, TimeSeriesDescription> tsDescMap = new HashMap<>();
		tsDescMap.put("R", descR);
		tsDescMap.put("U1", descU1);
		tsDescMap.put("U2", descU2);
		tsDescMap.put("U3", descU3);
		tsDescMap.put("D1", descD1);
		tsDescMap.put("D2", descD2);
		tsDescMap.put("DDD", descDDD);
		tsDescMap.put("UUU", descUUU);

		Map<String, Set<String>>  derivedTsMap = new HashMap<>();
		derivedTsMap.put("R", new HashSet<>(Arrays.asList("D1", "D2")));
		derivedTsMap.put("U1", new HashSet<>(Arrays.asList("R")));
		derivedTsMap.put("U2", new HashSet<>(Arrays.asList("R")));
		derivedTsMap.put("U3", new HashSet<>(Arrays.asList("R")));
		derivedTsMap.put("D1", new HashSet<>(Arrays.asList("DD1")));
		derivedTsMap.put("D2", new HashSet<>(Arrays.asList("DD1")));
		derivedTsMap.put("DDD", new HashSet<>());
		derivedTsMap.put("UUU", new HashSet<>(Arrays.asList("UU1", "UU2")));

		List<DerivationNode> result = service.buildNodes(procMap, tsDescMap, derivedTsMap);
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(result.size(), 11);

		for(DerivationNode node : result) {
			String id = node.getUniqueId();
			List<Processor> procList = procMap.get(id);
			TimeSeriesDescription desc = tsDescMap.get(id);
			Set<String> derivedTs = derivedTsMap.get(id);
			Processor nodeProc = null;

			// Identify the correct source processor, if applicable
			if(procList != null && !procList.isEmpty()) {
				for(Processor proc : procList) {
					if(proc.getProcessorPeriod().getEndTime().equals(node.getPeriodEndTime()) && 
						proc.getProcessorPeriod().getStartTime().equals(node.getPeriodStartTime())) {
							nodeProc = proc;
							break;
						}
				}
				assertNotNull(nodeProc);
				assertEquals(node.getInputTimeSeriesUniqueIds(), nodeProc.getInputTimeSeriesUniqueIds());
			}
			
			assertEquals(node.getUniqueId(), desc.getUniqueId());

			// Validate derived ts, if applicable
			if(derivedTs != null && !derivedTs.isEmpty()) {
				assertThat(node.getDerivedTimeSeriesUniqueIds(), containsInAnyOrder(derivedTs.toArray()));
			}
			
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void buildDerivationChainTest() {
		given(tsUidsService.getTimeSeriesUniqueIdList(any(String.class))).willReturn(fullIdList);

		// Build expected batch description request/responses
		List<List<TimeSeriesDescription>> responses = new ArrayList<>();
		responses.add(new ArrayList<>());
		Integer subListIndex = 0;
		for(Integer i = 0; i < fullDescList.size(); i++) {
			responses.get(subListIndex).add(fullDescList.get(i));
			if((i+1) % DerivationChainBuilderService.MAX_TS_DESC_QUERY_SIZE == 0) {
				subListIndex++;
				responses.add(new ArrayList<>());
			}
		}
		when(descService.getTimeSeriesDescriptionList(any(List.class))).thenAnswer(new Answer<List<TimeSeriesDescription>>() {
			private int responseIndex = 0;

			public List<TimeSeriesDescription> answer(InvocationOnMock invocation) {
				List<TimeSeriesDescription> result = null;
				result = responses.get(responseIndex);
				responseIndex++;
				return result;
			}
		});

		// Execute Test
		List<DerivationNode> result = service.buildDerivationChain("R", "location");
		assertNotNull(result);
		assertTrue(!result.isEmpty());
		assertEquals(result.size(), 25);
	}

	public CompletableFuture<List<Processor>> mockAsyncProcs(List<Processor> list) {
		CompletableFuture<List<Processor>> returnVal = new CompletableFuture<>();
		returnVal.obtrudeValue(list);
		return returnVal;
	}
}
