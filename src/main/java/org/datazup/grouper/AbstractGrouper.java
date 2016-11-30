package org.datazup.grouper;

import org.apache.commons.lang3.math.NumberUtils;
import org.datazup.expression.NullObject;
import org.datazup.grouper.exceptions.GroupingException;
import org.datazup.grouper.exceptions.NotValidMetric;
import org.datazup.grouper.utils.GroupUtils;
import org.datazup.utils.Tuple;

import java.util.*;

/**
 * Created by ninel on 11/30/16.
 */
public abstract class AbstractGrouper implements IGrouper{


    protected abstract Number handleMinMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;

    protected abstract Number handleMaxMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;

    protected abstract Number handleAvgMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;

    protected abstract Number handleSumMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;

    protected abstract Map<String,String> getRawReport(String reportName) throws Exception;

    protected abstract Number handleCountMetric(String reportName, String fieldKey) throws Exception;

    @Override
    public Map<String, Object> upsert(String reportName, DimensionKey dimensionKey, List<String> metrics) {
        List<Tuple<String,Object>> tupleList = dimensionKey.getDimensionValues();

        // Map<GroupKey,Map<String,Number>> groupedByDimensionAsKey = new HashMap<>();

        Map<String,Object> report = dimensionKey.getDimensionValuesMap();
        if (null==report || report.size()==0)
            return null;

        for (String metric: metrics){
            Tuple<String, MetricType> metricType = GroupUtils.parseMetricType(metric);
            // increment field in reportName - hash - in Redis
            String fieldKey = getFieldKey(tupleList);
            Object metricValueObject = dimensionKey.evaluate(metricType.getKey());

            if (metricValueObject instanceof NullObject || null==metricValueObject){
                // TODO: metricValueObject is sometimes NullObject - what we should do? - do we need to remove and not to count or to count NullObjects as well
                continue;
            }

            fieldKey+=("^"+metric);

            Number result =  null;

            try {
                result = upsert(reportName, fieldKey, metricType.getValue(), metricValueObject);
            } catch (Exception e) {
                throw new GroupingException("Problem upserting report: "+reportName+" for field: "+fieldKey+" metric: "+metric);
            }

            if (null==result){
                throw new NotValidMetric("Invalid metric upserted result - it shouldn't be null");
            }

            report.put(GroupUtils.normalizeKey(metric), result);
        }

        return report;
    }

    private Number upsert(String reportName, String fieldKey, MetricType metricType, Object metricValueObject) throws Exception {
        Number result =  null;
        if (metricType.equals(MetricType.COUNT)){
            result = handleCountMetric(reportName, fieldKey);
        }else {
            if (!(metricValueObject instanceof Number)){
                throw new NotValidMetric("Invalid metric value: "+metricValueObject+" for metric: "+metricType);
            }
            Number metricValue  = (Number)metricValueObject;
            switch (metricType) {
                case SUM:
                    result = handleSumMetric(reportName, fieldKey, metricValue);
                    break;
                case AVG:
                    result = handleAvgMetric(reportName, fieldKey, metricValue);
                    break;
                case MAX:
                    result = handleMaxMetric(reportName, fieldKey, metricValue);
                    break;
                case MIN:
                    result = handleMinMetric(reportName, fieldKey, metricValue);
                    break;
                default:
                    throw new NotValidMetric("Invalid metric: " + metricType);
            }
        }
        return result;

    }



    private String getFieldKey(List<Tuple<String, Object>> tupleList) {
        StringBuilder sb = new StringBuilder();
        for (Tuple<String,Object> t: tupleList){
            sb.append(t.toString());
            sb.append(":");
        }
        return sb.toString();
    }



    @Override
    public List<Map<String, Object>> getReportList(String reportName, List<String> dimensions, List<String> metrics) {
        List<Map<String, Object>>  result = getGroupedSimpleReportList(reportName, dimensions, metrics);
        return result;
    }

    private List<Map<String,Object>> getGroupedSimpleReportList(String reportName, List<String> dimensions, List<String> metrics){
        try {

            List<Map<String,Object>> result = new ArrayList<>();
            Map<String,String> reportMap = getRawReport(reportName);
            if (null==reportMap)
                throw new GroupingException("There is no report: "+reportName);

            Map<GroupKey,Map<String,Number>> groupedByDimensionAsKey = new HashMap<>();
            GroupKey dimensionMap = new GroupKey();
            Map<String,Number> metricMap = new HashMap<>();

            Set<String> processedDimensions = new HashSet<>();
            Set<String> processedMetrics = new HashSet<>();


            for (String key: reportMap.keySet()) {
                String valueStr = reportMap.get(key);
                Number metricKeyValueNumber = NumberUtils.createNumber(valueStr);

                // have to group by dimension keys

                String[] splitted = key.split(":");

                for (String splitKey: splitted){
                    if (splitKey.contains("^")){ // check metrics
                        String metric = splitKey.substring(1);
                        Tuple<String, MetricType> metricType = GroupUtils.parseMetricType(metric);
                        String metricKeyName = metricType.getValue().toString()+"("+metricType.getKey()+")";
                        if (metrics.contains(metricKeyName)) {
                            processedMetrics.add(metricKeyName);
                            metricMap.put(metricType.getValue() + GroupUtils.normalizeKey(metricType.getKey()), metricKeyValueNumber);
                        }
                    }else if (splitKey.contains("(")){
                        String functionKey =   splitKey.substring(0, splitKey.indexOf("("));
                        String fieldName = splitKey.substring(splitKey.indexOf("$")+1,splitKey.lastIndexOf("$"));

                        String fieldValueStr = splitKey.substring(splitKey.lastIndexOf(")")+1);
                        Object resolvedValue = GroupUtils.resolveValue(fieldValueStr);

                        String fieldKeyName = GroupUtils.toFunctionKey(functionKey, fieldName);
                        if (dimensions.contains(fieldKeyName)){
                            processedDimensions.add(fieldKeyName);
                            String normalizedKey = GroupUtils.normalizeKey(fieldKeyName);
                            dimensionMap.put(normalizedKey, resolvedValue);
                        }

                    }else {
                        String fieldKey = splitKey.substring(0, splitKey.lastIndexOf("$") + 1);
                        String fieldValue = splitKey.substring(splitKey.lastIndexOf("$") + 1);
                        Object resolvedValue = GroupUtils.resolveValue(fieldValue);

                        if (dimensions.contains(fieldKey)) {
                            processedDimensions.add(fieldKey);
                            String normalizedKey = GroupUtils.normalizeKey(fieldKey);
                            dimensionMap.put(normalizedKey, resolvedValue);
                        }
                    }
                }

                if (processedDimensions.size()==dimensions.size()){
                    Map<String,Number> metricInGroupedMap = null;
                    if (groupedByDimensionAsKey.containsKey(dimensionMap)){
                        metricInGroupedMap = groupedByDimensionAsKey.get(dimensionMap);
                    }else{
                        metricInGroupedMap = new HashMap<>();
                    }

                    metricInGroupedMap.putAll(metricMap);
                    groupedByDimensionAsKey.put(dimensionMap, metricInGroupedMap);


                }
                metricMap = new HashMap<>();
                dimensionMap = new GroupKey();
            }

            for (GroupKey key: groupedByDimensionAsKey.keySet()){
                Map<String,Number> metric = groupedByDimensionAsKey.get(key);
                Map<String,Object> obj = new HashMap<>();
                obj.putAll(key.getKeyValueMap());
                obj.putAll(metric);

                result.add(obj);
            }

            return result;
        }catch (Exception e) {
            throw new GroupingException("Cannot execute report: "+reportName, e);
        }

    }


}
