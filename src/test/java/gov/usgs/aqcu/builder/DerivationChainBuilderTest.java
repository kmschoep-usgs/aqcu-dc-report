package gov.usgs.aqcu.builder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import gov.usgs.aqcu.model.DerivationNode;
import gov.usgs.aqcu.retrieval.AsyncDerivationChainRetrievalService;
import gov.usgs.aqcu.retrieval.DownchainProcessorListService;
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
	private DownchainProcessorListService downProcService;
	@MockBean
	private AsyncDerivationChainRetrievalService asyncService;

	private DerivationChainBuilderService service;

	CompletableFuture<List<Processor>> mockEmptyFuture = new CompletableFuture<>();
	List<String> fullIdList = Arrays.asList("R", "D1", "D2", "U1", "U2", "UU", "UD", "DU", "DD", "UUU", "UUD", "UDU", "UDD", "DUU", "DUD", "DDU", "DDD");
	Processor procRA = new Processor();
	Processor procU1A = new Processor();
	Processor procU2A = new Processor();
	Processor procD1A = new Processor();
	Processor procD2A = new Processor();
	Processor procUUA = new Processor();
	Processor procUDA = new Processor();
	Processor procDUA = new Processor();
	Processor procDDA = new Processor();
	Processor procUUDA = new Processor();
	Processor procUDDA = new Processor();
	Processor procDUDA = new Processor();
	Processor procDDDA = new Processor();

	TimeSeriesDescription descR = new TimeSeriesDescription();
	TimeSeriesDescription descU1 = new TimeSeriesDescription();
	TimeSeriesDescription descU2 = new TimeSeriesDescription();
	TimeSeriesDescription descD1 = new TimeSeriesDescription();
	TimeSeriesDescription descD2 = new TimeSeriesDescription();
	TimeSeriesDescription descUU = new TimeSeriesDescription();
	TimeSeriesDescription descUD = new TimeSeriesDescription();
	TimeSeriesDescription descDU = new TimeSeriesDescription();
	TimeSeriesDescription descDD = new TimeSeriesDescription();
	TimeSeriesDescription descUUU = new TimeSeriesDescription();
	TimeSeriesDescription descUUD = new TimeSeriesDescription();
	TimeSeriesDescription descUDU = new TimeSeriesDescription();
	TimeSeriesDescription descUDD = new TimeSeriesDescription();
	TimeSeriesDescription descDUU = new TimeSeriesDescription();
	TimeSeriesDescription descDUD = new TimeSeriesDescription();
	TimeSeriesDescription descDDU = new TimeSeriesDescription();
	TimeSeriesDescription descDDD = new TimeSeriesDescription();

	TimeRange procPeriodA;
	TimeRange procPeriodB;
	TimeRange procPeriodC;

	DerivationNode nodeRA;
	DerivationNode nodeU1A;
	DerivationNode nodeU2A;
	DerivationNode nodeD1A;
	DerivationNode nodeD2A;
	DerivationNode nodeUUA;
	DerivationNode nodeUDA;
	DerivationNode nodeDUA;
	DerivationNode nodeDDA;
	DerivationNode nodeUUUA;
	DerivationNode nodeUUDA;
	DerivationNode nodeUDUA;
	DerivationNode nodeUDDA;
	DerivationNode nodeDUUA;
	DerivationNode nodeDUDA;
	DerivationNode nodeDDUA;
	DerivationNode nodeDDDA;

	@Before
	public void setup() {
		//Builder Service
		service = new DerivationChainBuilderService(tsUidsService, descService, downProcService, asyncService);

		mockEmptyFuture.obtrudeValue(new ArrayList<>());

		//Build test chain time ranges
		procPeriodA = new TimeRange();
		procPeriodA.setStartTime(Instant.parse("2016-01-01T00:00:00Z"));
		procPeriodA.setEndTime(Instant.parse("2017-01-01T00:00:00Z"));
		procPeriodB = new TimeRange();
		procPeriodB.setStartTime(Instant.parse("2017-01-01T00:00:00Z"));
		procPeriodB.setEndTime(Instant.parse("2018-01-01T00:00:00Z"));
		procPeriodC = new TimeRange();
		procPeriodC.setStartTime(Instant.parse("2018-01-01T00:00:00Z"));
		procPeriodC.setEndTime(Instant.parse("2019-01-01T00:00:00Z"));

		//Build test chain Processors
		procRA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("U1", "U2")));
		procRA.setOutputTimeSeriesUniqueId("R");
		procRA.setProcessorPeriod(procPeriodA);
		procU1A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU")));
		procU1A.setOutputTimeSeriesUniqueId("U1");
		procU1A.setProcessorPeriod(procPeriodA);
		procU2A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU")));
		procU2A.setOutputTimeSeriesUniqueId("U2");
		procU2A.setProcessorPeriod(procPeriodA);
		procD1A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("R", "DU")));
		procD1A.setOutputTimeSeriesUniqueId("D1");
		procD1A.setProcessorPeriod(procPeriodA);
		procD2A.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("R")));
		procD2A.setOutputTimeSeriesUniqueId("D2");
		procD2A.setProcessorPeriod(procPeriodA);
		procUUA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UUU")));
		procUUA.setOutputTimeSeriesUniqueId("UU");
		procUUA.setProcessorPeriod(procPeriodA);
		procUDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("U2", "UDU")));
		procUDA.setOutputTimeSeriesUniqueId("UD");
		procUDA.setProcessorPeriod(procPeriodA);
		procDUA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("DUU")));
		procDUA.setOutputTimeSeriesUniqueId("DU");
		procDUA.setProcessorPeriod(procPeriodA);
		procDDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("D1", "D2", "DDU")));
		procDDA.setOutputTimeSeriesUniqueId("DD");
		procDDA.setProcessorPeriod(procPeriodA);
		procUUDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UU")));
		procUUDA.setOutputTimeSeriesUniqueId("UUD");
		procUUDA.setProcessorPeriod(procPeriodA);
		procUDDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("UD")));
		procUDDA.setOutputTimeSeriesUniqueId("UDD");
		procUDDA.setProcessorPeriod(procPeriodA);
		procDUDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("DU")));
		procDUDA.setOutputTimeSeriesUniqueId("DUD");
		procDUDA.setProcessorPeriod(procPeriodA);
		procDDDA.setInputTimeSeriesUniqueIds(new ArrayList<>(Arrays.asList("DD")));
		procDDDA.setOutputTimeSeriesUniqueId("DDD");
		procDDDA.setProcessorPeriod(procPeriodA);

		//Build test chain Descriptions
		descR.setUniqueId("R");
		descU1.setUniqueId("U1");
		descU2.setUniqueId("U2");
		descD1.setUniqueId("D1");
		descD2.setUniqueId("D2");
		descUU.setUniqueId("UU");
		descUD.setUniqueId("UD");
		descDU.setUniqueId("DU");
		descDD.setUniqueId("DD");
		descUUU.setUniqueId("UUU");
		descUUD.setUniqueId("UUD");
		descUDU.setUniqueId("UDU");
		descUDD.setUniqueId("UDD");
		descDUU.setUniqueId("DUU");
		descDUD.setUniqueId("DUD");
		descDDU.setUniqueId("DDU");
		descDDD.setUniqueId("DDD");

		//Build test chain DerivationNodes
		nodeRA = new DerivationNode(procRA, descR, new HashSet<>(Arrays.asList("D1", "D2")));
		nodeU1A = new DerivationNode(procU1A, descU1, new HashSet<>(Arrays.asList("R")));
		nodeU2A = new DerivationNode(procU2A, descU2, new HashSet<>(Arrays.asList("R", "UD")));
		nodeD1A = new DerivationNode(procD1A, descD1, new HashSet<>(Arrays.asList("DD")));
		nodeD2A = new DerivationNode(procD2A, descD2, new HashSet<>(Arrays.asList("DD")));
		nodeUUA = new DerivationNode(procUUA, descUU, new HashSet<>(Arrays.asList("U1", "U2", "UUD")));
		nodeUDA = new DerivationNode(procUDA, descUD, new HashSet<>(Arrays.asList("UDD")));
		nodeDUA = new DerivationNode(procDUA, descDU, new HashSet<>(Arrays.asList("DUD", "D1")));
		nodeDDA = new DerivationNode(procDDA, descDD, new HashSet<>(Arrays.asList("DDD")));
		nodeUUUA = new DerivationNode(null, descUUU, new HashSet<>(Arrays.asList("UU")));
		nodeUUDA = new DerivationNode(procUUDA, descUUD, new HashSet<>(Arrays.asList()));
		nodeUDUA = new DerivationNode(null, descUDU, new HashSet<>(Arrays.asList("UD")));
		nodeUDDA = new DerivationNode(procUDDA, descUDD, new HashSet<>(Arrays.asList()));
		nodeDUUA = new DerivationNode(null, descDUU, new HashSet<>(Arrays.asList("DU")));
		nodeDUDA = new DerivationNode(procDUDA, descDUD, new HashSet<>(Arrays.asList()));
		nodeDDUA = new DerivationNode(null, descDDU, new HashSet<>(Arrays.asList("DD")));
		nodeDDDA = new DerivationNode(procDDDA, descDDD, new HashSet<>(Arrays.asList()));

		//Build test chain async responses
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockAsyncProcs(Arrays.asList(procRA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("R")).willReturn(mockAsyncProcs(Arrays.asList(procD1A, procD2A)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("U1")).willReturn(mockAsyncProcs(Arrays.asList(procU1A)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("U1")).willReturn(mockAsyncProcs(Arrays.asList(procRA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("U2")).willReturn(mockAsyncProcs(Arrays.asList(procU2A)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("U2")).willReturn(mockAsyncProcs(Arrays.asList(procRA, procUDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("D1")).willReturn(mockAsyncProcs(Arrays.asList(procD1A)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("D1")).willReturn(mockAsyncProcs(Arrays.asList(procDDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("D2")).willReturn(mockAsyncProcs(Arrays.asList(procD2A)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("D2")).willReturn(mockAsyncProcs(Arrays.asList(procDDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UU")).willReturn(mockAsyncProcs(Arrays.asList(procUUA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UU")).willReturn(mockAsyncProcs(Arrays.asList(procU1A, procU2A, procUUDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UD")).willReturn(mockAsyncProcs(Arrays.asList(procUDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UD")).willReturn(mockAsyncProcs(Arrays.asList(procUDDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DU")).willReturn(mockAsyncProcs(Arrays.asList(procDUA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DU")).willReturn(mockAsyncProcs(Arrays.asList(procDUDA, procD1A)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DD")).willReturn(mockAsyncProcs(Arrays.asList(procDDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DD")).willReturn(mockAsyncProcs(Arrays.asList(procDDDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UUU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UUU")).willReturn(mockAsyncProcs(Arrays.asList(procUUA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UUD")).willReturn(mockAsyncProcs(Arrays.asList(procUUDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UUD")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UDU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UDU")).willReturn(mockAsyncProcs(Arrays.asList(procUDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("UDD")).willReturn(mockAsyncProcs(Arrays.asList(procUDDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("UDD")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DUU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DUU")).willReturn(mockAsyncProcs(Arrays.asList(procDUA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DUD")).willReturn(mockAsyncProcs(Arrays.asList(procDUDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DUD")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DDU")).willReturn(mockEmptyFuture);
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DDU")).willReturn(mockAsyncProcs(Arrays.asList(procDDA)));
		given(asyncService.getAsyncUpchainProcessorListByTimeSeriesUniqueId("DDD")).willReturn(mockAsyncProcs(Arrays.asList(procDDDA)));
		given(asyncService.getAsyncDownchainProcessorListByTimeSeriesUniqueId("DDD")).willReturn(mockEmptyFuture);
	}

	@Test
	public void getRecursiveProcessorMapInclusiveSingleTimesTest() {
		Map<String, List<Processor>> result = service.getRecursiveProcessorMap("R", fullIdList);
		assertTrue(result != null);
		assertTrue(!result.isEmpty());
		assertEquals(17, result.keySet().size());
		assertEquals(17, result.values().size());
		assertThat(result.keySet(), containsInAnyOrder(fullIdList.toArray()));
		assertTrue(result.get("R") != null);
		assertThat(result.get("R"), containsInAnyOrder(procRA));
		assertTrue(result.get("U1") != null);
		assertThat(result.get("U1"), containsInAnyOrder(procU1A));
		assertTrue(result.get("U2") != null);
		assertThat(result.get("U2"), containsInAnyOrder(procU2A));
		assertTrue(result.get("D1") != null);
		assertThat(result.get("D1"), containsInAnyOrder(procD1A));
		assertTrue(result.get("D2") != null);
		assertThat(result.get("D2"), containsInAnyOrder(procD2A));
		assertTrue(result.get("UU") != null);
		assertThat(result.get("UU"), containsInAnyOrder(procUUA));
		assertTrue(result.get("UD") != null);
		assertThat(result.get("UD"), containsInAnyOrder(procUDA));
		assertTrue(result.get("DU") != null);
		assertThat(result.get("DU"), containsInAnyOrder(procDUA));
		assertTrue(result.get("DD") != null);
		assertThat(result.get("DD"), containsInAnyOrder(procDDA));
		assertTrue(result.get("UUU") != null);
		assertTrue(result.get("UUU").isEmpty());
		assertTrue(result.get("UUD") != null);
		assertThat(result.get("UUD"), containsInAnyOrder(procUUDA));
		assertTrue(result.get("UDU") != null);
		assertTrue(result.get("UDU").isEmpty());
		assertTrue(result.get("UDD") != null);
		assertThat(result.get("UDD"), containsInAnyOrder(procUDDA));
		assertTrue(result.get("DUU") != null);
		assertTrue(result.get("DUU").isEmpty());
		assertTrue(result.get("DUD") != null);
		assertThat(result.get("DUD"), containsInAnyOrder(procDUDA));
		assertTrue(result.get("DDU") != null);
		assertTrue(result.get("DDU").isEmpty());
		assertTrue(result.get("DDD") != null);
		assertThat(result.get("DDD"), containsInAnyOrder(procDDDA));
	}

	@Test
	public void getRecursiveProcessorMapLimitedSingleTimesTest() {
		Map<String, List<Processor>> result = service.getRecursiveProcessorMap("R", Arrays.asList("R", "U1", "U2", "D1", "D2", "UU", "DD", "UUU", "DDD"));
		assertTrue(result != null);
		assertTrue(!result.isEmpty());
		assertEquals(13, result.keySet().size());
		assertEquals(13, result.values().size());
		assertThat(result.keySet(), containsInAnyOrder("R", "U1", "U2", "D1", "D2", "UU", "UD", "DU", "DD", "UUU", "UUD", "DDU", "DDD"));
		assertTrue(result.get("R") != null);
		assertThat(result.get("R"), containsInAnyOrder(procRA));
		assertTrue(result.get("U1") != null);
		assertThat(result.get("U1"), containsInAnyOrder(procU1A));
		assertTrue(result.get("U2") != null);
		assertThat(result.get("U2"), containsInAnyOrder(procU2A));
		assertTrue(result.get("D1") != null);
		assertThat(result.get("D1"), containsInAnyOrder(procD1A));
		assertTrue(result.get("D2") != null);
		assertThat(result.get("D2"), containsInAnyOrder(procD2A));
		assertTrue(result.get("UU") != null);
		assertThat(result.get("UU"), containsInAnyOrder(procUUA));
		assertTrue(result.get("UD") != null);
		assertThat(result.get("UD"), containsInAnyOrder(procUDA));
		assertTrue(result.get("DU") != null);
		assertThat(result.get("DU"), containsInAnyOrder(procDUA));
		assertTrue(result.get("DD") != null);
		assertThat(result.get("DD"), containsInAnyOrder(procDDA));
		assertTrue(result.get("UUU") != null);
		assertTrue(result.get("UUU").isEmpty());
		assertTrue(result.get("UUD") != null);
		assertThat(result.get("UUD"), containsInAnyOrder(procUUDA));
		assertTrue(result.get("UDU") == null);
		assertTrue(result.get("UDD") == null);
		assertTrue(result.get("DUU") == null);
		assertTrue(result.get("DUD") == null);
		assertTrue(result.get("DDU") != null);
		assertTrue(result.get("DDU").isEmpty());
		assertTrue(result.get("DDD") != null);
		assertThat(result.get("DDD"), containsInAnyOrder(procDDDA));
	}

	public CompletableFuture<List<Processor>> mockAsyncProcs(List<Processor> list) {
		CompletableFuture<List<Processor>> returnVal = new CompletableFuture<>();
		returnVal.obtrudeValue(list);
		return returnVal;
	}
}
