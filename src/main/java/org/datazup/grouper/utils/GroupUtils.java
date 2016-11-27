package org.datazup.grouper.utils;

import org.apache.commons.lang3.math.NumberUtils;
import org.datazup.grouper.MetricType;
import org.datazup.utils.Tuple;

/**
 * Created by ninel on 11/27/16.
 */
public class GroupUtils {

    public static Tuple<String, MetricType> parseMetricType(String metric) {

        String metricName = null;
        String metricField = null;

        metricName = metric.substring(0, metric.indexOf("("));
        metricField = metric.substring(metric.indexOf("$")+1);
        metricField = metricField.substring(0, metricField.indexOf("$"));

        metricField = "$"+metricField+"$";

        Tuple<String, MetricType> tuple = new Tuple<>(metricField, MetricType.valueOf(metricName));
        return tuple;
    }

    public static String normalizeKey(String key) {
        String normal = key.replaceAll("\\$","").replaceAll("\\(|\\)", "");
        return normal;
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

    public static String toFunctionKey(String function, String fieldName) {
        return function+"($"+fieldName+"$)";
    }
}
