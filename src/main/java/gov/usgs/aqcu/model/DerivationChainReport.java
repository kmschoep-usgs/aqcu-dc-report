package gov.usgs.aqcu.model;

import java.util.List;
import java.util.ArrayList;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.RatingCurve;

public class DerivationChainReport {	
	private List<DerivationNode> derivationsInChain;
	private DerivationChainReportMetadata reportMetadata;
	
	
	public DerivationChainReport() {
		reportMetadata = new DerivationChainReportMetadata();
		derivationsInChain = new ArrayList();
	}

	public List<DerivationNode> getDerivationsInChain() {
		return derivationsInChain;
	}
	
	public DerivationChainReportMetadata getReportMetadata() {
		return reportMetadata;
	}

	public void setDerivationsInChain(List<DerivationNode> derivationsInChain) {
		this.derivationsInChain = derivationsInChain;
	}
	
	public void setReportMetadata(DerivationChainReportMetadata val) {
		reportMetadata = val;
	}
}
	
