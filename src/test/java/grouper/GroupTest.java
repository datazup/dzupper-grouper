/*
package grouper;

import base.TestBase;
import org.datazup.pathextractor.PathExtractor;
import org.datazup.utils.DateTimeUtils;
import org.datazup.utils.JsonUtils;
import org.datazup.utils.Tuple;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

*/
/**
 * Created by ninel on 11/25/16.
 *//*

public class GroupTest extends TestBase {

    @Test
    public void groupByProofTestWithJsonDefinition(){
        List<Map<String,Object>> records = getNestedListOfMaps();

        */
/*String def = "{"
                +"\"dimensions\":[\"$name$\", \"$type$\"], "
                +"\"metrics\":[\"SUM($amount$)\", \"COUNT($amount$)\"], "
                +"\"time\":{"
                +"\"field\": \"$date$\", "
                +"\"reportOn\":[\"hour\",\"day\",\"month\"]"
                +" } "
                +"}";*//*


        String def = "{"
                +"\"dimensions\":[{\"name\":\"$name$\", \"type\":\"string\"}, {\"name\":\"$type$\", \"type\":\"string\"},"
                +"{\"name\":\"HOUR($date$)\", \"type\":\"int\"},{\"name\":\"DAY($date$)\", \"type\":\"int\"},{\"name\":\"MONTH($date$)\", \"type\":\"int\"},{\"name\":\"YEAR($date$)\", \"type\":\"int\"}],"

                +"\"metrics\":[{\"name\":\"SUM($amount$)\", \"type\":\"int\"},{\"name\":\"COUNT($amount$)\", \"type\":\"int\"}] "
                +"}";

        Map<String,Object> mapDefinition = JsonUtils.getMapFromJson(def);



        Map<Map<String,Object>, Map<String,Object>> tmpR = new HashMap<>();

        List<String> dimensions = (List<String>) mapDefinition.get("dimensions");
        List<String> metrics = (List<String>) mapDefinition.get("metrics");
        Map<String, Object> timeDimension = (Map<String, Object>) mapDefinition.get("time");

        for (Map<String,Object> streamMap: records){

            Map<String,Object> keyValue = getKeyValue(dimensions, tmpR, streamMap);
            Map<Tuple<String, String>,Object> metricValues = getMetricValues(metrics, tmpR, streamMap);
            Map<String,Object> timeDimKeyValues = getTimeDimensionValues(timeDimension, tmpR, streamMap);

            Map<String,Object> keyTimeDimValues = new HashMap<>();
            keyTimeDimValues.putAll(keyValue);
            keyTimeDimValues.putAll(timeDimKeyValues);

            Map<String,Object> metricInResult = null;

            if (tmpR.containsKey(keyTimeDimValues)){
                metricInResult = tmpR.get(keyTimeDimValues);
            }

            metricInResult = updateMetrics(metricInResult, metricValues);

            //resultOfGrouping.putAll(metricInResult);
            tmpR.put(keyTimeDimValues, metricInResult);

        }

        List<Map<String, Object>> resultOfGrouping = prepareResult(tmpR);

        System.out.println(JsonUtils.getJsonFromObject(resultOfGrouping));

        Assert.assertNotNull(resultOfGrouping);
        Assert.assertTrue(resultOfGrouping.size()>0);

    }

    private List<Map<String, Object>> prepareResult(Map<Map<String, Object>, Map<String, Object>> tmpR) {
        List<Map<String,Object>> list = new ArrayList<>();

        for (Map<String,Object> m1: tmpR.keySet()){
            Map<String,Object> m2 = tmpR.get(m1);

            Map<String,Object> r = new HashMap<>();
            r.putAll(m1);
            r.putAll(m2);

            list.add(r);
        }

        return list;
    }

    private Map<String, Object> updateMetrics(Map<String, Object> metricInResult, Map<Tuple<String, String>, Object> metricValues) {
        if (null==metricInResult){
            metricInResult = new HashMap<>();
        }

        for (Tuple<String, String> metricKey: metricValues.keySet()){
            Object metricValue = metricValues.get(metricKey);
            String inMetricKey = metricKey.toString();
            switch (metricKey.getKey()){
                case "SUM":
                    Double sum = (Double) metricInResult.get(inMetricKey);
                    if (null==sum)
                        sum = 0.0;

                    Double newValue = getDouble(metricValue);
                    sum+=newValue;

                    metricInResult.put(inMetricKey, sum);

                    break;
                case "COUNT":
                    Double count = (Double) metricInResult.get(inMetricKey);
                    if (null==count)
                        count = 0d;
                    ++count;
                    metricInResult.put(inMetricKey, count);
                    break;
                case "AVG":
                    Double avg = (Double) metricInResult.get(inMetricKey);
                    if (null==avg)
                        avg = 0d;

                    Double newAvgValue = getDouble(metricValue);

                    Double prevCount = getPreviousCount(metricKey.getValue());


                    break;
                case "MIN":
                    Double min = (Double) metricInResult.get(inMetricKey);
                    Double val = getDouble(metricValue);
                    min = Math.min(min, val);
                    metricInResult.put(inMetricKey, min);
                    break;
                case "MAX":
                    Double max = (Double) metricInResult.get(inMetricKey);
                    Double valmax = getDouble(metricValue);
                    min = Math.min(max, valmax);
                    metricInResult.put(inMetricKey, max);
                    break;
            }
        }

        return metricInResult;
    }

    private Double getPreviousCount(String value) {
        return null;
    }

    private Double getDouble(Object val) {
        Double d = null;
        if (val instanceof Integer){
            Integer i = (Integer)val;
            d = i.doubleValue();
        }else if (val instanceof Long){
            Long l = (Long)val;
            d = l.doubleValue();
        }else if (val instanceof Double){
            d = (Double)val;
        }
        return d;
    }

    private Map<String, Object> getKeyValue(List<String> dimensions, Map<Map<String, Object>, Map<String, Object>> tmpR, Map<String, Object> streamMap) {
        Map<String,Object> keyValues = new HashMap<>();
        PathExtractor pathExtractor = new PathExtractor(streamMap);
        for (String dim: dimensions){
            Object value = pathExtractor.extractObjectValue(dim);// "$field1.field2$"... $user.location$ ...
            keyValues.put(dim, value);
        }

        return keyValues;
    }

    private Map<Tuple<String, String>,Object> getMetricValues(List<String> metrics, Map<Map<String, Object>, Map<String, Object>> tmpR, Map<String, Object> streamMap) {
        Map<Tuple<String, String>,Object> metricKeyValues = new HashMap<>();
        PathExtractor pathExtractor = new PathExtractor(streamMap);
        for (String metric: metrics){
            Tuple<String, String> metricNameField = getMetricNameField(metric);
            Object value = pathExtractor.extractObjectValue(metricNameField.getValue());
            metricKeyValues.put(metricNameField, value);
        }

        return metricKeyValues;
    }

    private Tuple<String, String> getMetricNameField(String metric) {
        String metricName = null;
        String metricField = null;

        metricName = metric.substring(0, metric.indexOf("("));
        metricField = metric.substring(metric.indexOf("$")+1);
        metricField = metricField.substring(0, metricField.indexOf("$"));

        metricField = "$"+metricField+"$";

        Tuple<String, String> tuple = new Tuple<>(metricName, metricField);
        return tuple;
    }


    private Map<String,Object> getTimeDimensionValues(Map<String, Object> timeDimension, Map<Map<String, Object>, Map<String, Object>> tmpR, Map<String, Object> streamMap) {
        Map<String,Object> timeDimKeyValues = new HashMap<>();
        PathExtractor pathExtractor = new PathExtractor(streamMap);

        String fieldName =  (String) timeDimension.get("field");
        List<String> reportOn = (List<String>)timeDimension.get("reportOn");

        if (null!=reportOn && null!=fieldName) {
            Object value = pathExtractor.extractObjectValue(fieldName);
            DateTime dateValue = DateTimeUtils.resolve(value);

            for (String dtReport: reportOn){
                switch (dtReport){
                    case "hour":
                        timeDimKeyValues.put(dtReport, dateValue.getHourOfDay());
                        break;
                    case "day":
                        timeDimKeyValues.put(dtReport, dateValue.getDayOfMonth());
                        break;
                    case "month":
                        timeDimKeyValues.put(dtReport, dateValue.getMonthOfYear());
                        break;
                    case "year":
                        timeDimKeyValues.put(dtReport, dateValue.getYear());
                        break;
                    case "total":
                        break;
                }
            }

        }


        return timeDimKeyValues;
    }

}
*/
