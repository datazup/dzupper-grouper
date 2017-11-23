package org.datazup.grouper;

import org.apache.commons.lang3.math.NumberUtils;
import org.datazup.expression.NullObject;
import org.datazup.grouper.exceptions.GroupingException;
import org.datazup.grouper.exceptions.NotValidMetric;
import org.datazup.grouper.utils.GroupUtils;
import org.datazup.utils.Tuple;

import java.util.*;

/**
 * Created by admin@datazup on 11/30/16.
 */
public abstract class AbstractGrouper implements IGrouper{


    protected abstract Number handleMinMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;

    protected abstract Number handleMaxMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;

    protected abstract Number handleAvgMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;

    protected abstract Number handleSumMetric(String reportName, String fieldKey, Number metricValue)  throws Exception;
    
    protected abstract Object handleLastMetric(String reportName, String fieldKey, Object metricValue)  throws Exception;

    protected abstract Map<String,String> getRawReport(String reportName) throws Exception;

    protected abstract Number handleCountMetric(String reportName, String fieldKey) throws Exception;

    @Override
    public List<Map<String, Object>> upsert(String reportName, DimensionKey dimensionKey, List<Map<String,String>> metrics) {
        List<List<Tuple<Map<String,String>,Object>>> r = dimensionKey.getTupleListDimensions();

        List<Map<String, Object>> resultMap = new ArrayList<>();
        for (List<Tuple<Map<String,String>,Object>> tuples: r){
            Map<String, Object> reportMap = getDimensionReportMap(tuples); //new HashMap<>();
            for (Map<String, String> metric : metrics) {
            	String func = metric.get("name");
            	if(metric.containsKey("func")){
            		func = metric.get("func");
            	}
            	
                Tuple<String, MetricType> metricType = GroupUtils.parseMetricType(func.toString());
                String fieldKey = GroupUtils.getFieldKey(tuples);

                Object metricValueObject = dimensionKey.evaluate(metricType.getKey());

                if (metricValueObject instanceof NullObject || null == metricValueObject) {
                    // TODO: metricValueObject is sometimes NullObject - what we should do? - do we need to remove and not to count or to count NullObjects as well
                    continue;
                }
                fieldKey += ("^" + metric.get("name"));
                Object result = null;
                try {
                    result = upsert(reportName, fieldKey, metricType.getValue(), metricValueObject);
                } catch (Exception e) {
                    throw new GroupingException("Problem upserting report: " + reportName + " for field: " + fieldKey + " metric: " + metric.get("name")+" metricValue: "+metricValueObject, e);
                }
                if (null == result) {
                    throw new NotValidMetric("Invalid metric upserted result - it shouldn't be null");
                }
                reportMap.put(GroupUtils.normalizeKey(metric.get("name").toString()), result);
            }
            resultMap.add(reportMap);
        }

        return  resultMap;
    }

    private Map<String, Object> getDimensionReportMap(List<Tuple<Map<String, String>, Object>> tuples) {
        Map<String, Object> map = new HashMap<>();

        for (Tuple<Map<String, String>, Object> tuple: tuples){
            String key = GroupUtils.normalizeKey(tuple.getKey().get("name"));
            map.put(key, tuple.getValue());
        }

        return map;
    }


    private Object upsert(String reportName, String fieldKey, MetricType metricType, Object metricValueObject) throws Exception {
        Object result =  null;
        if (metricType.equals(MetricType.COUNT)){
            result = handleCountMetric(reportName, fieldKey);
        }
        else if (metricType.equals(MetricType.LAST)){
        	result = handleLastMetric(reportName, fieldKey, metricValueObject);
        }
        else {
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

    @Override
    public List<Map<String, Object>> getReportList(String reportName, List<Map<String,String>> dimensions, List<Map<String,String>> metrics) {
        List<Map<String, Object>>  result = getGroupedSimpleReportList(reportName, dimensions, metrics);
        return result;
    }

    private List<Map<String,Object>> getGroupedSimpleReportList(String reportName, List<Map<String,String>> dimensions, List<Map<String,String>> metrics){
        try {

            List<Map<String,Object>> result = new ArrayList<>();
            Map<String,String> reportMap = getRawReport(reportName);
            if (null==reportMap)
                throw new GroupingException("There is no report: "+reportName);

            Map<GroupKey,Map<String,Number>> groupedByDimensionAsKey = new HashMap<>();
            GroupKey dimensionMap = new GroupKey();
            Map<String,Number> metricMap = new HashMap<>();

            Set<String> processedDimensions = new HashSet<>();

            Map<String,Map<String,String>> mapKeyDimensionKeyValue = new HashMap<String,Map<String,String>>();
            for (Map<String, String> dimKeyVal: dimensions){
            	String key = dimKeyVal.get("name");
            	mapKeyDimensionKeyValue.put(key, dimKeyVal);
            }
            
            Map<String,Map<String,String>> mapKeyDimensionKeyValueMetrics = new HashMap<String,Map<String,String>>();
            for (Map<String, String> dimKeyVal: metrics){
            	String key = dimKeyVal.get("name");
            	mapKeyDimensionKeyValueMetrics.put(key, dimKeyVal);
            }
            
            Map<String, Tuple<String, MetricType>> nest = null;
            

            for (String key: reportMap.keySet()) {
                String valueStr = reportMap.get(key);
                Number metricKeyValueNumber = NumberUtils.createNumber(valueStr);

                // have to group by dimension keys

                String[] splitted = key.split(":");

                for (String splitKey: splitted){
                    if (splitKey.contains("^")){ // check metrics
                        String metric = splitKey.substring(1);
                        if(mapKeyDimensionKeyValueMetrics.containsKey(metric)){
                        	String metricKeyName = "";
                        	if((mapKeyDimensionKeyValueMetrics.get(metric)).containsKey("func")){
                        		metricKeyName = metric;
                        		metric = (mapKeyDimensionKeyValueMetrics.get(metric)).get("func");
                        	}
                        	
                        	Tuple<String, MetricType> metricType = GroupUtils.parseMetricType(metric);
                            
                            metricKeyName = metricKeyName == "" ? metricType.getValue() + GroupUtils.normalizeKey(metricType.getKey()) :metricKeyName;
                            metricMap.put(metricKeyName, metricKeyValueNumber);
                        }
                        
                    }
                    else{
                    	String fieldKeyName  ="";
                    	Object resolvedValue = null;
                    	if(splitKey.contains("*")){
                    		String functionKey =   splitKey.substring(0, splitKey.indexOf("*"));

                            String fieldValueStr = splitKey.substring(splitKey.lastIndexOf("*")+1);
                            resolvedValue = GroupUtils.resolveValue(fieldValueStr);
                            
                            if(mapKeyDimensionKeyValue.containsKey(functionKey)){
                            	if((mapKeyDimensionKeyValue.get(functionKey)).containsKey("func")){
                            		splitKey = (mapKeyDimensionKeyValue.get(functionKey)).get("func");
                            		fieldKeyName = functionKey;
                            	}
                            }
                    	}
                    		
                    	if (splitKey.contains("(")){
                            String functionKey =   splitKey.substring(0, splitKey.indexOf("("));
                            String fieldName = splitKey.substring(splitKey.indexOf("$")+1,splitKey.lastIndexOf("$"));

                            if(resolvedValue == null){
                            	String fieldValueStr = splitKey.substring(splitKey.lastIndexOf(")")+1);
                                resolvedValue = GroupUtils.resolveValue(fieldValueStr);
                            }

                            fieldKeyName = fieldKeyName == "" ? GroupUtils.toFunctionKey(functionKey, fieldName) : fieldKeyName;
                            if (mapKeyDimensionKeyValue.containsKey(fieldKeyName)){
                                processedDimensions.add(fieldKeyName);
                                String normalizedKey = GroupUtils.normalizeKey(fieldKeyName);
                                dimensionMap.put(normalizedKey, resolvedValue);
                            }
                    	}
                    	else{
                    		fieldKeyName =  fieldKeyName == "" ? splitKey.substring(0, splitKey.lastIndexOf("$") + 1) : fieldKeyName;
                    		
                    		if(resolvedValue == null){
                    			String fieldValue = splitKey.substring(splitKey.lastIndexOf("$") + 1);
                                resolvedValue = GroupUtils.resolveValue(fieldValue);
                    		}
                            
                            if (mapKeyDimensionKeyValue.containsKey(fieldKeyName)) {
                                processedDimensions.add(fieldKeyName);
                                String normalizedKey = GroupUtils.normalizeKey(fieldKeyName);
                                dimensionMap.put(normalizedKey, resolvedValue);
                            }
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
