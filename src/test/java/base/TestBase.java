package base;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ninel on 11/25/16.
 */

public abstract class TestBase {

    protected List<Map<String,Object>> getListOfMaps(){
        List<Map<String,Object>> list1 = new ArrayList<>();
        for (int i = 0;i<10;i++) {
            Map<String,Object> o = new HashMap<>();
            o.put("price", 10*i);
            o.put("name", "item"+(i%2));
            o.put("type", "type"+(i%2));
            o.put("amount",2*(10-i));


            list1.add(o);
        }
        return list1;
    }

    protected List<Map<String,Object>> getNestedListOfMaps(){
        List<Map<String,Object>> list1 = new ArrayList<>();
        for (int i = 0;i<10;i++) {
            Map<String,Object> o = new HashMap<>();
            o.put("price", 10*i);
            o.put("date", DateTime.now().minusDays(i%3));
            o.put("name", "item"+(i%2));
            o.put("type", "type"+(i%2));
            o.put("amount",2*(10-i));

            List<Map<String,Object>> list2 = new ArrayList<>();

            for (int j = 0;j<10;j++) {
                Map<String, Object> r = new HashMap<>();
                r.put("price1", 10 * j);
                r.put("name1", "item" + (j % 2));
                r.put("type1", "type" + (j % 2));
                r.put("amount1", 2 * (10 - j));

                list2.add(r);
            }
            o.put("child", list2);

            list1.add(o);
        }
        return list1;
    }



    public Map<String, Object> getData() {
        Map<String, Object> child = new HashMap<>();
        child.put("name", "child");
        child.put("value", 1);

        Map<String, Object> parent = new HashMap<>();

        List<Object> list = new ArrayList<>();
        list.add("Hello");
        list.add("Hi");
        list.add("Nice");

        Map<String, Object> mp = new HashMap<>();
        mp.put("first", 1);
        mp.put("second", "hah");

        Map<String, Object> cmp = new HashMap<>();
        cmp.put("thirdChild", "yupi");

        List<Object> ll = new ArrayList<>();
        ll.add("thirdhaha");
        ll.add("thirdopa");
        ll.add("thirdjope");
        cmp.put("thirdlist", ll);

        mp.put("third", cmp);

        List<Object> l = new ArrayList<>();
        l.add("childHello");
        l.add("child2Hello");
        l.add("child3Hello");
        mp.put("fourth", l);
        list.add(mp);

        List<Object> list1 = new ArrayList<>();
        list1.add((new HashMap<>().put("n", "n")));

        child.put("list", list);


        parent.put("child", child);
        parent.put("list", list1);
        return parent;
    }

}
