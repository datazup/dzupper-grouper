package org.datazup.grouper;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by admin@datazup on 11/27/16.
 */
public class GroupKey {
    private Map<String,Object> keyValueMap;

    public GroupKey(){
        keyValueMap = new HashMap<>();
    }
    public GroupKey(Map<String,Object> keyValues){
        this.keyValueMap = keyValues;
    }

    public void put(String key, Object value){
        this.keyValueMap.put(key, value);
    }

    public Map<String,Object> getKeyValueMap() {
        return keyValueMap;
    }

    public int hashCode() {
        // we need this hash to uniquely reprepsent the message by messageId
        HashCodeBuilder hashCodeBuilder = new HashCodeBuilder(17, 31); // two randomly chosen prime numbers
        // if deriving: appendSuper(super.hashCode()).
        for (Map.Entry<String,Object> entry: keyValueMap.entrySet()){
            hashCodeBuilder.append(entry.getKey());
            hashCodeBuilder.append(entry.getValue());
        }

        return hashCodeBuilder.toHashCode();
    }

    public boolean equals(Object obj){
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof GroupKey))
            return false;

        GroupKey rhs = (GroupKey) obj;
        if (rhs.keyValueMap.size()!=this.keyValueMap.size())
            return false;

        EqualsBuilder equalBuilder =  new EqualsBuilder();

        List<String> rhsKeys = new ArrayList<>(rhs.keyValueMap.keySet());
        List<String> thisKeys = new ArrayList<>(this.keyValueMap.keySet());
        equalBuilder.append(thisKeys, rhsKeys);
        for (int i=0;i<rhsKeys.size();i++){
            equalBuilder.append(this.keyValueMap.get(thisKeys.get(i)), rhs.keyValueMap.get(rhsKeys.get(i)));
        }

        return equalBuilder.isEquals();
    }
}
