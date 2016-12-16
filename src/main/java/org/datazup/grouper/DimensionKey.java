package org.datazup.grouper;

import org.datazup.expression.SelectMapperEvaluator;
import org.datazup.grouper.exceptions.GroupingException;
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
    private List<Map<String,String>> dimensions = null;
    private PathExtractorBase pathExtractor;
    private List<Tuple<Map<String,String>,Object>> tupleList;

    static SelectMapperEvaluator evaluator = SelectMapperEvaluator.getInstance();
    public DimensionKey(List<Map<String,String>> dimensions, PathExtractorBase pathExtractor) {
        this.dimensions = dimensions;
        this.pathExtractor = pathExtractor;
    }

    public List<Map<String,String>> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<Map<String,String>> dimensions) {
        this.dimensions = dimensions;
    }

    public void build() {
        tupleList= new ArrayList<>();
        for (Map<String,String> dimension: dimensions){
            Object value = evaluator.evaluate(dimension.get("name").toString(), pathExtractor);
            if (null!=value) {
                    Tuple<Map<String,String>, Object> tuple = new Tuple<>(dimension, value);
                    tupleList.add(tuple);
            }
        }
    }

    public List<List<Tuple<Map<String,String>,Object>>> getTupleListDimensions(){
        List<List<Tuple<Map<String,String>,Object>>> list = new ArrayList<>();
        List<List<Tuple<Map<String,String>,Object>>> arrayList = new ArrayList<>();

        Map<String, List<Tuple<Map<String,String>,Object>>> arrMap = new HashMap<>(); //- key as Field Name = value as lis of items - we shold have "nulls" in the items so that we can match by the index of the list

        boolean hasList = false;
        for (Tuple<Map<String,String>, Object> tuple: tupleList){

            Object tupleValue = tuple.getValue();
            if (null!=tupleValue) {
                String normalizedKeyName = tuple.getKey().get("name"); //GroupUtils.normalizeKey(tuple.getKey().get("name").toString());
                if (tupleValue instanceof List){
                    //if (hasList) throw new GroupingException("There could be only one dimension with List result. Please check dimensions");

                    hasList = true;
                    List l = (List)tupleValue;
                    List<Tuple<Map<String,String>,Object>> setList = new ArrayList<>();
                    for (Object o: l){
                        if (o instanceof List || o instanceof Map)
                            throw new GroupingException("The leaf can be Simple value not collection or map. Please check dimensions");
                        Tuple<Map<String,String>,Object> map = new Tuple<>(tuple.getKey(), o);
                        setList.add(map);

                    }
                    arrMap.put(normalizedKeyName, setList);

                }else{
                    Tuple<Map<String,String>,Object> map = new Tuple<>(tuple.getKey(), tupleValue);
                    List<Tuple<Map<String,String>,Object>> setList = new ArrayList<>();
                    setList.add(map);
                    list.add(setList);
                }

            }
        }
        if (arrMap.size()>0){

            Map<Integer, List<Tuple<Map<String,String>,Object>>> positionMap = new HashMap<>();

            for (String arrMapKeyField: arrMap.keySet()){
                List<Tuple<Map<String,String>, Object>> tupleValues = arrMap.get(arrMapKeyField);

                for (int i = 0;i<tupleValues.size();i++){
                    List<Tuple<Map<String,String>,Object>> atPosList = null;
                    if (positionMap.containsKey(i)){
                        atPosList = positionMap.get(i);
                    }else{
                        atPosList = new ArrayList<>();
                    }
                    atPosList.add(tupleValues.get(i));
                    positionMap.put(i, atPosList);
                }
            }

            for (Integer pos: positionMap.keySet()){
                List<Tuple<Map<String,String>,Object>> listTuples = positionMap.get(pos);
                for (List<Tuple<Map<String,String>,Object>> simpleListTuple: list){
                    listTuples.addAll(simpleListTuple);
                }
                arrayList.add(listTuples);
            }
            return arrayList;
        }else {
            List<Tuple<Map<String,String>,Object>> tmp = new ArrayList<>();
            for (List<Tuple<Map<String,String>,Object>> tuples: list){
                tmp.addAll(tuples);
            }
            List<List<Tuple<Map<String,String>,Object>>> tmpList = new ArrayList<>();
            tmpList.add(tmp);
            return tmpList;
        }
    }

   /* public Set<Tuple<String, Object>> getDimensionValuesMap(){
        Set<Tuple<String, Object>> list = new HashSet<>();

        if (null==tupleList){
            throw new DimensionKeyException("Dimension keys not yet built");
        }
        for (Tuple<Map<String,String>, Object> tuple: tupleList){
            if (null!=tuple.getValue()) {
                if (tuple.getValue() instanceof List){
                    List lst = (List)tuple.getValue();
                    for (Object ol: lst){
                        String normalizedKeyName = GroupUtils.normalizeKey(tuple.getKey().get("name").toString());
                        Tuple<String,Object> map = new Tuple<>(normalizedKeyName, ol);

                        list.add(map);
                    }
                }else {
                    String normalizedKeyName = GroupUtils.normalizeKey(tuple.getKey().get("name").toString());
                    Tuple<String,Object> map = new Tuple<>(normalizedKeyName, tuple.getValue());
                    list.add(map);
                }
            }
        }
        return list;
    }*/

    public Object evaluate(String expression){
        return evaluator.evaluate(expression, pathExtractor);
    }

    public String getDimensionFullKeyString(Map<String, Object> resultMap) {
        StringBuilder sb = new StringBuilder();
        for (Map<String, String> dim: dimensions){
            String key = dim.get("name");
            String normalized = GroupUtils.normalizeKey(key);
            if (resultMap.containsKey(normalized)){
                Object o = resultMap.get(normalized);
                sb.append(key).append(":").append(o).append(":");
            }
        }
        return sb.toString();
    }
}
