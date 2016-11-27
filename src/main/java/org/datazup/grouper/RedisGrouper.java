package org.datazup.grouper;

import org.apache.commons.lang3.math.NumberUtils;
import org.datazup.grouper.exceptions.GroupingException;
import org.datazup.grouper.exceptions.NotValidMetric;
import org.datazup.redis.RedisClient;
import org.datazup.utils.Tuple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Created by ninel on 11/25/16.
 */
@Component
public class RedisGrouper implements IGrouper{
    @Autowired
    RedisClient redisClient;

    @Override
    public Map<String, Object> upsert(String reportName, DimensionKey dimensionKey, List<String> metrics) {
        List<Tuple<String,Object>> tupleList = dimensionKey.getDimensionValues();

        Map<String,Object> report = dimensionKey.getDimensionValuesMap();
        for (String metric: metrics){
            Tuple<String, MetricType> metricType = parseMetricType(metric);
            // increment field in reportName - hash - in Redis
            String fieldKey = getFieldKey(tupleList);
            Object metricValueObject = dimensionKey.evaluate(metricType.getKey());

            if (!(metricValueObject instanceof Number)){
                throw new NotValidMetric("Invalid metric value: "+metricValueObject+" for metric: "+metricType);
            }
            Number metricValue = (Number)metricValueObject;

            fieldKey+=("^"+metric);

            Number result =  null;

            try {
                result = upsert(reportName, fieldKey, metricType.getValue(), metricValue);
            } catch (Exception e) {
                throw new GroupingException("Problem upserting report: "+reportName+" for field: "+fieldKey+" metric: "+metric);
            }

            if (null==result){
                throw new NotValidMetric("Invalid metric upserted result - it shouldn't be null");
            }

            report.put(metric, result);
        }

        return report;
    }

    private Number upsert(String reportName, String fieldKey, MetricType metricType, Number metricValue) throws Exception {
        Number result =  null;
        switch (metricType){
            case SUM:
                result = handleSumMetric(reportName, fieldKey, metricValue);
                break;
            case COUNT:
                result = handleCountMetric(reportName, fieldKey, metricValue);
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
                throw new NotValidMetric("Invalid metric: "+metricType);
        }
        return result;
    }

    private Number handleMinMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
        String valStr = redisClient.getFromHash(reportName, fieldKey);
        Number n = 0;
        if (!valStr.equalsIgnoreCase("nil")){
            n = NumberUtils.createNumber(valStr);
        }
        if (metricValue.doubleValue()<n.doubleValue()){
            redisClient.addToHash(reportName, fieldKey, metricValue.toString());
            return metricValue;
        }else{
            return n;
        }
    }

    private Number handleMaxMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
        String valStr = redisClient.getFromHash(reportName, fieldKey);
        Number n = 0;
        if (!valStr.equalsIgnoreCase("nil")){
            n = NumberUtils.createNumber(valStr);
        }
        if (metricValue.doubleValue()>n.doubleValue()){
            redisClient.addToHash(reportName, fieldKey, metricValue.toString());
            return metricValue;
        }else{
            return n;
        }
    }

    private Number handleAvgMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
        Number valAvgCount = handleCountMetric(reportName, fieldKey+":count", metricValue);
        Number valAvgSum = handleSumMetric(reportName, fieldKey+":sum", metricValue);

        // TODO: this should be refactored such that we'll use LUA script to calculate sum/count/avg/min/max,etc data and reuse

        Double avg = valAvgSum.doubleValue()/valAvgCount.doubleValue();

        redisClient.addToHash(reportName, fieldKey, avg.toString());

        return avg;
    }

    private Number handleCountMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
        Long d = redisClient.incrementHashFieldByValue(reportName, fieldKey, 1l);
        return d;
    }

    private Number handleSumMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
        Double d = redisClient.incrementHashFieldByFloatValue(reportName, fieldKey, metricValue.doubleValue());
        return d;
    }

    private String getFieldKey(List<Tuple<String, Object>> tupleList) {
        StringBuilder sb = new StringBuilder();
        for (Tuple<String,Object> t: tupleList){
            sb.append(t.toString());
            sb.append(":");
        }
        return sb.toString();
    }

    private Tuple<String, MetricType> parseMetricType(String metric) {

        String metricName = null;
        String metricField = null;

        metricName = metric.substring(0, metric.indexOf("("));
        metricField = metric.substring(metric.indexOf("$")+1);
        metricField = metricField.substring(0, metricField.indexOf("$"));

        metricField = "$"+metricField+"$";

        Tuple<String, MetricType> tuple = new Tuple<>(metricField, MetricType.valueOf(metricName));
        return tuple;
    }

    @Override
    public List<Map<String, Object>> getReportList(String reportName, List<String> dimensions, List<String> metrics) {
        List<Map<String, Object>>  result = getGroupedSimpleReportList(reportName, dimensions, metrics);
        return result;
    }

    private List<Map<String,Object>> getGroupedSimpleReportList(String reportName, List<String> dimensions, List<String> metrics){
        try {

            List<Map<String,Object>> result = new ArrayList<>();
            Map<String,String> reportMap = redisClient.getFromHash(reportName);
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
                        Tuple<String, MetricType> metricType = parseMetricType(metric);
                        String metricKeyName = metricType.getValue().toString()+"("+metricType.getKey()+")";
                        if (metrics.contains(metricKeyName)) {
                            processedMetrics.add(metricKeyName);
                            metricMap.put(metricType.getValue() + normalizeKey(metricType.getKey()), metricKeyValueNumber);
                        }
                    }else if (splitKey.contains("(")){
                        String functionKey =   splitKey.substring(0, splitKey.indexOf("("));
                        String fieldName = splitKey.substring(splitKey.indexOf("$")+1,splitKey.lastIndexOf("$"));

                        String fieldValueStr = splitKey.substring(splitKey.lastIndexOf(")")+1);
                        Object resolvedValue = resolveValue(fieldValueStr);

                        String fieldKeyName = toFunctionKey(functionKey, fieldName);
                        if (dimensions.contains(fieldKeyName)){
                            processedDimensions.add(fieldKeyName);
                            String normalizedKey = normalizeKey(fieldKeyName);
                            dimensionMap.put(normalizedKey, resolvedValue);
                        }

                    }else {
                        String fieldKey = splitKey.substring(0, splitKey.lastIndexOf("$") + 1);
                        String fieldValue = splitKey.substring(splitKey.lastIndexOf("$") + 1);
                        Object resolvedValue = resolveValue(fieldValue);

                        if (dimensions.contains(fieldKey)) {
                            processedDimensions.add(fieldKey);
                            String normalizedKey = normalizeKey(fieldKey);
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

    private String toFunctionKey(String function, String fieldName) {
        return function+"($"+fieldName+"$)";
    }

    @Deprecated
    private List<Map<String,Object>> getSimpleReportList(String reportName){

        try {
            List<Map<String,Object>> result = new ArrayList<>();
            Map<String,String> reportMap =   redisClient.getFromHash(reportName);
            if (null==reportMap)
                throw new GroupingException("There is no report: "+reportName);

            for (String key: reportMap.keySet()){
                String valueStr = reportMap.get(key);
                Number metricKeyValueNumber = NumberUtils.createNumber(valueStr);

                String[] splitted = key.split(":");
                Map<String,Object> record = new HashMap<>();
                for (String splitKey: splitted){
                    if (splitKey.contains("^")){
                        String metric = splitKey.substring(1);
                        Tuple<String, MetricType> metricType = parseMetricType(metric);
                        record.put(metricType.getValue()+normalizeKey(metricType.getKey()), metricKeyValueNumber);
                    }else if (splitKey.contains("(")){
                        String functionKey =   splitKey.substring(0, splitKey.indexOf("("));
                        String fieldName = splitKey.substring(splitKey.indexOf("$")+1,splitKey.lastIndexOf("$")); //splitKey.substring(splitKey.indexOf("$"));
                        // fieldName = fieldName.substring(0, fieldName.indexOf("$"));
                        String fieldValueStr = splitKey.substring(splitKey.lastIndexOf(")")+1);
                        Object resolvedValue = resolveValue(fieldValueStr);

                        String fieldKeyName = functionKey+fieldName;
                        record.put(fieldKeyName, resolvedValue);

                    }else{
                        String fieldKey = splitKey.substring(1, splitKey.lastIndexOf("$"));
                        String fieldValue = splitKey.substring(splitKey.lastIndexOf("$")+1);
                        Object resolvedValue = resolveValue(fieldValue);
                        record.put(fieldKey, resolvedValue);

                    }
                }
                result.add(record);
            }

            return result;

        } catch (Exception e) {
            throw new GroupingException("Cannot execute report: "+reportName, e);
        }

    }

    private String normalizeKey(String key) {
        String normal = key.replaceAll("\\$","").replaceAll("\\(|\\)", "");
        return normal;
    }

    private Object resolveValue(String fieldValueStr) {
        Object resolvedValue = fieldValueStr;
        try{
            resolvedValue = NumberUtils.createNumber(fieldValueStr);
        }catch (Exception e){}
        if (null==resolvedValue){
            resolvedValue = fieldValueStr;
        }
        return resolvedValue;
    }
}
