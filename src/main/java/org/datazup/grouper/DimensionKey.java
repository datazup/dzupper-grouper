package org.datazup.grouper;

import org.datazup.expression.SelectMapperEvaluator;
import org.datazup.grouper.exceptions.DimensionKeyException;
import org.datazup.grouper.utils.GroupUtils;
import org.datazup.pathextractor.PathExtractorBase;
import org.datazup.utils.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ninel on 11/25/16.
 */
public class DimensionKey {
    private List<String> dimensions = null;
    private PathExtractorBase pathExtractor;
    private List<Tuple<String,Object>> tupleList;
    static SelectMapperEvaluator evaluator = SelectMapperEvaluator.getInstance();
    public DimensionKey(List<String> dimensions, PathExtractorBase pathExtractor) {
        this.dimensions = dimensions;
        this.pathExtractor = pathExtractor;
        //setStreamMap(streamMap);
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public Map<String, Object> getStreamMap() {
        return pathExtractor.getDataObject();
    }

    /*public void setStreamMap(Map<String, Object> streamMap) {
        pathExtractor = new PathExtractor(streamMap);
    }*/

    public void build() {
        tupleList = new ArrayList<>();
        for (String dimension: dimensions){
            Object value = evaluator.evaluate(dimension, pathExtractor);
            if (null!=value) {
                Tuple<String, Object> tuple = new Tuple<>(dimension, value);
                tupleList.add(tuple);
            }
        }
    }
    public List<Tuple<String,Object>> getDimensionValues(){
        return tupleList;
    }

    public Map<String,Object> getDimensionValuesMap(){
        Map<String,Object> map = new HashMap<>();
        if (null==tupleList){
            throw new DimensionKeyException("Dimension keys not yet built");
        }
        for (Tuple<String, Object> tuple: tupleList){
            if (null!=tuple.getValue())
                map.put(GroupUtils.normalizeKey(tuple.getKey()), tuple.getValue());
        }
        return map;
    }

    public Object evaluate(String expression){
        return evaluator.evaluate(expression, pathExtractor);
    }
}
