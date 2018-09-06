package gov.usgs.aqcu.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.HashSet;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Processor;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

import org.junit.Test;

import gov.usgs.aqcu.retrieval.TimeSeriesDescriptionListServiceTest;
import gov.usgs.aqcu.retrieval.UpchainProcessorListServiceTest;
import gov.usgs.aqcu.util.TimeSeriesUtils;



public class DerivationNodeTest {
	 TimeSeriesDescription desc1 = TimeSeriesDescriptionListServiceTest.DESC_1;
	 Processor proc1 = UpchainProcessorListServiceTest.PROC_A;

	 @Test
	 public void constructorTest() {
		  DerivationNode result1 = new DerivationNode(proc1, desc1, new HashSet<>(Arrays.asList("test1")));
		  assertTrue(result1 != null);
		  assertEquals(result1.getComputation(), desc1.getComputationIdentifier());
		  assertThat(result1.getDerivedTimeSeriesUniqueIds(), containsInAnyOrder("test1"));
		  assertEquals(result1.getDescription(), desc1.getDescription());
		  assertEquals(result1.getIdentifier(), desc1.getIdentifier());
		  assertThat(result1.getInputTimeSeriesUniqueIds(), containsInAnyOrder(proc1.getInputTimeSeriesUniqueIds().toArray()));
		  assertEquals(result1.getLocation(), desc1.getLocationIdentifier());
		  assertEquals(result1.getParameter(), desc1.getParameter());
		  assertEquals(result1.getParameterIdentifier(), desc1.getParameter());
		  assertEquals(result1.getPeriod(), desc1.getComputationPeriodIdentifier());
		  assertEquals(result1.getPeriodEndTime(), proc1.getProcessorPeriod().getEndTime());
		  assertEquals(result1.getPeriodStartTime(), proc1.getProcessorPeriod().getStartTime());
		  assertEquals(result1.getPrimary(), Boolean.valueOf(TimeSeriesUtils.isPrimaryTimeSeries(desc1)).toString());
		  assertEquals(result1.getPublish(), desc1.isPublish().toString());
		  assertEquals(result1.getSubLocation(), desc1.getSubLocationIdentifier());
		  assertEquals(result1.getTimeSeriesType(), desc1.getTimeSeriesType());
		  assertEquals(result1.getUniqueId(), desc1.getUniqueId());
	 }

	 @Test
	 public void constructorNullTest() {
		  Boolean failed = false;
		  try {
				new DerivationNode(null, null, null);
		  } catch(NullPointerException e) {
				failed = true;
		  }
		  if(!failed) {
				fail("Expected null pointer exception");
		  }

		  failed = false;
		  try {
				new DerivationNode(proc1, null, new HashSet<>(Arrays.asList("test1")));
		  } catch(NullPointerException e) {
				failed = true;
		  }
		  if(!failed) {
				fail("Expected null pointer exception");
		  }
		  
		  DerivationNode result1 = new DerivationNode(null, desc1, null);
		  assertTrue(result1 != null);
		  DerivationNode result2 = new DerivationNode(proc1, desc1, null);
		  assertTrue(result2 != null);
		  DerivationNode result3 = new DerivationNode(null, desc1, new HashSet<>(Arrays.asList("test1")));
		  assertTrue(result3 != null);
	 }
}