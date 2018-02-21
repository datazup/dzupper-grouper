package org.datazup.grouper;

import org.datazup.exceptions.GrouperException;

import java.util.List;
import java.util.Map;

/**
 * Created by admin@datazup on 11/25/16.
 */
public interface IGrouper {
    List<Map<String, Object>> upsert(String reportName, DimensionKey dimensionKey, List<Map<String,String>> metrics) throws GrouperException;
    List<Map<String,Object>> getReportList(String reportName, List<Map<String,String>> dimensions, List<Map<String,String>> metrics);
}
