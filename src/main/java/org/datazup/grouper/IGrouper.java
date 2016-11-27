package org.datazup.grouper;

import java.util.List;
import java.util.Map;

/**
 * Created by ninel on 11/25/16.
 */
public interface IGrouper {
    Map<String, Object> upsert(String reportName, DimensionKey dimensionKey, List<String> metrics);
    List<Map<String,Object>> getReportList(String reportName, List<String> dimensions, List<String> metrics);
}
