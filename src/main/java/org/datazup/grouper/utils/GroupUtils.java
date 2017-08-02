package org.datazup.grouper.utils;

import org.apache.commons.lang3.math.NumberUtils;
import org.datazup.grouper.MetricType;
import org.datazup.utils.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by ninel on 11/27/16.
 */
public class GroupUtils {

    public static Tuple<String, MetricType> parseMetricType(String metric) {

        String metricName = null;
        String metricField = null;

        metricName = metric.substring(0, metric.indexOf("("));
        if (metric.contains("$")) {
            metricField = metric.substring(metric.indexOf("$") + 1);
            metricField = metricField.substring(0, metricField.indexOf("$"));
        }else{
            metricField = metric.substring(metric.indexOf("(")+1, metric.indexOf(")"));
        }

        metricField = "$"+metricField+"$";

        Tuple<String, MetricType> tuple = new Tuple<>(metricField, MetricType.valueOf(metricName));
        return tuple;
    }

    public static String normalizeKey(String key) {
        String normal = key.replaceAll("\\$","").replaceAll("\\(|\\)", "");
        if (normal.contains(".")){
            normal = capitalizeDelimiter(normal, "."); //normal.replaceAll(".","#");
        }
        return normal;
    }

    public static String capitalizeDelimiter(String text, String delimiter){

        int pos = 0;
        boolean capitalize = false;
        StringBuilder sb = new StringBuilder(text);
        while (pos < sb.length()) {
            if (sb.charAt(pos) == '.') {
                capitalize = true;
            } else if (capitalize) { // && !Character.isWhitespace(sb.charAt(pos))
                sb.setCharAt(pos, Character.toUpperCase(sb.charAt(pos)));
                capitalize = false;
            }
            pos++;
        }
        String s = sb.toString();
        return s.replaceAll("\\.", "");
    }

    public static Object resolveValue(String fieldValueStr) {
        Object resolvedValue = fieldValueStr;
        try{
            resolvedValue = NumberUtils.createNumber(fieldValueStr);
        }catch (Exception e){}
        if (null==resolvedValue){
            resolvedValue = fieldValueStr;
        }
        return resolvedValue;
    }

    public static String getFieldKey(Map<String,Object>  reportKey, Object reportValue){
        StringBuilder sb = new StringBuilder();

        return sb.toString();
    }

    public static String getFieldKey(List<Tuple<Map<String,String>, Object>> tupleList) {
        StringBuilder sb = new StringBuilder();
        for (Tuple<Map<String,String>,Object> t: tupleList){
            
        	String name = t.getKey().get("name").toString();
        	if(!name.contains("$")){
        		name =name+"*";
        	}
            sb.append(name+ t.getValue());
            sb.append(":");
        }
        return sb.toString();
    }

    public static String getNormalizedFieldKey(List<Tuple<Map<String,String>, Object>> tupleList) {
        StringBuilder sb = new StringBuilder();
        for (Tuple<Map<String,String>,Object> t: tupleList){
            String normalized = normalizeKey(t.getKey().get("name").toString());
            sb.append(normalized+ t.getValue());
            sb.append(":");
        }
        return sb.toString();
    }

    public static String toFunctionKey(String function, String fieldName) {
        return function+"($"+fieldName+"$)";
    }
}
