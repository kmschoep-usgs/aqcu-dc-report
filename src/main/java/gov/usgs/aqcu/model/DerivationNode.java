package gov.usgs.aqcu.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.Processor;
import com.aquaticinformatics.aquarius.sdk.timeseries.servicemodels.Publish.TimeSeriesDescription;

import gov.usgs.aqcu.util.TimeSeriesUtils;

public class DerivationNode {
    private String processorType;
    private List<String> inputTimeSeriesUniqueIds;
    private List<String> derivedTimeSeriesUniqueIds;
    private String description;
    private String timeSeriesType;
    private String parameter;
    private String parameterIdentifier;
    private Instant periodStartTime;
    private String period;
    private Instant periodEndTime;
    private String computation;
    private String publish;
    private String uniqueId;
    private String primary;
    private String identifier;
    private String location;
    private String subLocation;

    public DerivationNode(Processor proc, TimeSeriesDescription tsDesc, Set<String> derivedTimeSeriesUniqueIds) {
        //Required
        parseTimeSeriesDescription(tsDesc);

        //Optional
        if(proc != null) {
            parseProcessor(proc);
        }

        //Optionsal
        if(derivedTimeSeriesUniqueIds != null && !derivedTimeSeriesUniqueIds.isEmpty()) {
            this.derivedTimeSeriesUniqueIds = new ArrayList<>(derivedTimeSeriesUniqueIds);
        }
    }

    public void parseProcessor(Processor proc) {
        this.processorType = proc.getProcessorType();
        this.inputTimeSeriesUniqueIds = proc.getInputTimeSeriesUniqueIds();
        this.periodStartTime = proc.getProcessorPeriod().getStartTime();
        this.periodEndTime = proc.getProcessorPeriod().getEndTime();
    }

    public void parseTimeSeriesDescription(TimeSeriesDescription tsDesc) {
        this.description = tsDesc.getDescription();
        this.timeSeriesType = tsDesc.getTimeSeriesType();
        this.parameter = tsDesc.getParameter();
        this.parameterIdentifier = tsDesc.getParameter();
        this.identifier = tsDesc.getIdentifier();
        this.location = tsDesc.getLocationIdentifier();
        this.subLocation = tsDesc.getSubLocationIdentifier();
        this.period = tsDesc.getComputationPeriodIdentifier();
        this.computation = tsDesc.getComputationIdentifier();
        this.publish = tsDesc.isPublish().toString();
        this.uniqueId = tsDesc.getUniqueId();
        this.primary = Boolean.valueOf(TimeSeriesUtils.isPrimaryTimeSeries(tsDesc)).toString();
    }

    //Getters
    public String getProcessorType() {
        return processorType;
    }
    public List<String> getInputTimeSeriesUniqueIds() {
        return inputTimeSeriesUniqueIds;
    }
    public List<String> getDerivedTimeSeriesUniqueIds() {
        return derivedTimeSeriesUniqueIds;
    }
    public String getDescription() {
        return description;
    }
    public String getTimeSeriesType() {
        return timeSeriesType;
    }
    public String getParameter() {
        return parameter;
    }
    public String getParameterIdentifier() {
        return parameterIdentifier;
    }
    public Instant getPeriodStartTime() {
        return periodStartTime;
    }
    public String getPeriod() {
        return period;
    }
    public Instant getPeriodEndTime() {
        return periodEndTime;
    }
    public String getComputation() {
        return computation;
    }
    public String getPublish() {
        return publish;
    }
    public String getUniqueId() {
        return uniqueId;
    }
    public String getPrimary() {
        return primary;
    }
    public String getIdentifier() {
        return identifier;
    }
    public String getLocation() {
        return location;
    }
    public String getSubLocation() {
        return subLocation;
    }

    //Setters
    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }
    public void setInputTimeSeriesUniqueIds(List<String> inputTimeSeriesUniqueIds) {
        this.inputTimeSeriesUniqueIds = inputTimeSeriesUniqueIds;
    }
    public void setDerivedTimeSeriesUniqueIds(List<String> derivedTimeSeriesUniqueIds) {
        this.derivedTimeSeriesUniqueIds = derivedTimeSeriesUniqueIds;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public void setTimeSeriesType(String timeSeriesType) {
        this.timeSeriesType = timeSeriesType;
    }
    public void setParameter(String parameter) {
        this.parameter = parameter;
    }
    public void setParameterIdentifier(String parameterIdentifier) {
        this.parameterIdentifier = parameterIdentifier;
    }
    public void setPeriodStartTime(Instant periodStartTime) {
        this.periodStartTime = periodStartTime;
    }
    public void setPeriod(String period) {
        this.period = period;
    }
    public void setPeriodEndTime(Instant periodEndTime) {
        this.periodEndTime = periodEndTime;
    }
    public void setComputation(String computation) {
        this.computation = computation;
    }
    public void setPublish(String publish) {
        this.publish = publish;
    }
    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }
    public void setPrimary(String primary) {
        this.primary = primary;
    }
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
    public void setLocation(String location) {
        this.location = location;
    }
    public void setSubLocation(String subLocation) {
        this.subLocation = subLocation;
    }
}