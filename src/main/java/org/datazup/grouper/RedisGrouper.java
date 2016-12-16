package org.datazup.grouper;

import org.apache.commons.lang3.math.NumberUtils;
import org.datazup.redis.RedisClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by ninel on 11/25/16.
 */
@Component
public class RedisGrouper extends AbstractGrouper{

    @Autowired
    RedisClient redisClient;

    public Map<String, String> getRawReport(String reportName) throws Exception {
        return redisClient.getFromHash(reportName);
    }

    public Number handleMinMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
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

    public Number handleMaxMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
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

    public Number handleAvgMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
        Number valAvgCount = handleCountMetric(reportName, fieldKey+":count");
        Number valAvgSum = handleSumMetric(reportName, fieldKey+":sum", metricValue);

        // TODO: this should be refactored such that we'll use LUA script to calculate sum/count/avg/min/max,etc data and reuse

        Double avg = valAvgSum.doubleValue()/valAvgCount.doubleValue();

        redisClient.addToHash(reportName, fieldKey, avg.toString());

        return avg;
    }

    public Number handleCountMetric(String reportName, String fieldKey) throws Exception {
        Long d = redisClient.incrementHashFieldByValue(reportName, fieldKey, 1l);
        return d;
    }

    public Number handleSumMetric(String reportName, String fieldKey, Number metricValue) throws Exception {
        Double d = redisClient.incrementHashFieldByFloatValue(reportName, fieldKey, metricValue.doubleValue());
        return d;
    }
    
    public Object handleLastMetric(String reportName, String fieldKey, Object metricValue) throws Exception {
    	Object d = redisClient.addToHash(reportName, fieldKey, metricValue.toString());
        return metricValue;
    }


}
