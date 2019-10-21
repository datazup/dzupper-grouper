package grouper;

import base.ModuleConfiguration;
import base.TestBase;
import org.datazup.exceptions.EvaluatorException;
import org.datazup.exceptions.GrouperException;
import org.datazup.expression.SelectMapperEvaluator;
import org.datazup.grouper.DimensionKey;
import org.datazup.grouper.GroupKey;
import org.datazup.grouper.IGrouper;
import org.datazup.pathextractor.PathExtractor;
import org.datazup.pathextractor.SimpleResolverHelper;
import org.datazup.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.RedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by admin@datazup on 11/25/16.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ModuleConfiguration.class)
public class DzupperRedisGrouperTest extends TestBase {

    @Autowired
    IGrouper grouper;

    @Autowired
    RedisClient redisClient;

    String reportName = "Company:custom:Report1";

    static SimpleResolverHelper mapListResolver = new SimpleResolverHelper();
    static SelectMapperEvaluator evaluator = SelectMapperEvaluator.getInstance(mapListResolver);

    private Map<String,Object> getReportDefinition(){
    		 
      	 String newDef = "{"
                 +"\"dimensions\":[{\"name\":\"$name$\", \"type\":\"string\"}, {\"name\":\"$type$\", \"type\":\"string\"},"
    			 +"{\"name\":\"Hour_Date\",\"func\":\"HOUR($date$)\", \"type\":\"int\"},{\"name\":\"DAY($date$)\", \"type\":\"int\"},{\"name\":\"MONTH($date$)\", \"type\":\"int\"},{\"name\":\"YEAR($date$)\", \"type\":\"int\"}],"
                
                 +"\"metrics\":[{ \"name\":\"Sum_Amount\",\"func\":\"SUM($amount$)\", \"type\":\"int\"},{\"name\":\"COUNT($amount$)\", \"type\":\"int\"}] "
                 +"}";
    	 
//        String def = "{"
//                +"\"dimensions\":[\"$name$\", \"$type$\", \"HOUR($date$)\", \"DAY($date$)\", \"MONTH($date$)\", \"YEAR($date$)\"], "
//                +"\"metrics\":[\"SUM($amount$)\", \"COUNT($amount$)\"] "
//                +"}";

        Map<String,Object> mapDefinition = JsonUtils.getMapFromJson(newDef);
        return mapDefinition;
    }

    private void processReport(List<Map<String,Object>> records, List<Map<String,String>> dimensions, List<Map<String,String>> metrics) throws EvaluatorException, GrouperException {
        long start = System.currentTimeMillis();
        for (Map<String,Object> streamMap: records){
            PathExtractor pathExtractor = new PathExtractor(streamMap,mapListResolver);
            DimensionKey dimensionKey = new DimensionKey(dimensions, pathExtractor, evaluator);
            dimensionKey.build();
            List<Map<String,Object>> currentMap = grouper.upsert(reportName, dimensionKey, metrics);

            for (Map<String,Object> resultMap: currentMap){
                String fullKey =  dimensionKey.getDimensionFullKeyString(resultMap);
            }
            //System.out.println("CurrentMap: "+ JsonUtils.getJsonFromObject(currentMap));
            /*Assert.assertTrue(currentMap.size()==8);
            Assert.assertTrue(currentMap.containsKey("SUMamount"));
            Assert.assertTrue(currentMap.containsKey("COUNTamount"));
            System.out.println("CurrentMap: "+ JsonUtils.getJsonFromObject(currentMap));*/
        }
        System.out.println("Processed: "+records.size()+" in: "+(System.currentTimeMillis()-start)+" ms with Assert and Serialization");
    }

    @Test
    public void izDzupperGroupingTheData() throws Exception {

        List<Map<String,Object>> records = getNestedListOfMaps();
        Map<String,Object> mapDefinition = getReportDefinition();

        redisClient.del(reportName);

        List<Map<String,String>> dimensions = (List<Map<String,String>>) mapDefinition.get("dimensions");
        List<Map<String,String>> metrics = (List<Map<String,String>>) mapDefinition.get("metrics");
        processReport(records, dimensions, metrics);

        List<Map<String, Object>> report = grouper.getReportList(reportName, dimensions, metrics);
        System.out.println("Full report: "+ JsonUtils.getJsonFromObject(report));

       // assertResultTrue(report);
    }

    private void assertResultTrue(List<Map<String, Object>> report) {
        Assert.assertNotNull(report);
        List<GroupKey> groupKeys = new ArrayList<>();
        for (Map<String,Object> map: report){
            GroupKey g = new GroupKey(map);
            groupKeys.add(g);
        }

        GroupKey g1 = new GroupKey();
        g1.put("name", "item0");
        g1.put("type", "type0");
        g1.put("HOURdate", 2);
        g1.put("DAYdate", 27);
        g1.put("MONTHdate", 11);
        g1.put("YEARdate", 2016);
        g1.put("SUMamount", 28);
        g1.put("COUNTamount",2);

        Assert.assertTrue(groupKeys.contains(g1));

        g1 = new GroupKey();
        g1.put("name", "item1");
        g1.put("type", "type1");
        g1.put("HOURdate", 2);
        g1.put("DAYdate", 26);
        g1.put("MONTHdate", 11);
        g1.put("YEARdate", 2016);
        g1.put("SUMamount", 24);
        g1.put("COUNTamount",2);

        Assert.assertTrue(groupKeys.contains(g1));

        g1 = new GroupKey();
        g1.put("name", "item0");
        g1.put("type", "type0");
        g1.put("HOURdate", 2);
        g1.put("DAYdate", 25);
        g1.put("MONTHdate", 11);
        g1.put("YEARdate", 2016);
        g1.put("SUMamount", 20);
        g1.put("COUNTamount",2);

        Assert.assertTrue(groupKeys.contains(g1));

        g1 = new GroupKey();
        g1.put("name", "item1");
        g1.put("type", "type1");
        g1.put("HOURdate", 2);
        g1.put("DAYdate", 27);
        g1.put("MONTHdate", 11);
        g1.put("YEARdate", 2016);
        g1.put("SUMamount", 16);
        g1.put("COUNTamount",2);

        Assert.assertTrue(groupKeys.contains(g1));

        g1 = new GroupKey();
        g1.put("name", "item0");
        g1.put("type", "type0");
        g1.put("HOURdate", 2);
        g1.put("DAYdate", 26);
        g1.put("MONTHdate", 11);
        g1.put("YEARdate", 2016);
        g1.put("SUMamount", 12);
        g1.put("COUNTamount",1);

        Assert.assertTrue(groupKeys.contains(g1));

        g1 = new GroupKey();
        g1.put("name", "item1");
        g1.put("type", "type1");
        g1.put("HOURdate", 2);
        g1.put("DAYdate", 25);
        g1.put("MONTHdate", 11);
        g1.put("YEARdate", 2016);
        g1.put("SUMamount", 10);
        g1.put("COUNTamount",1);

        Assert.assertTrue(groupKeys.contains(g1));

    }

    @Test
    public void performanceGroupingTest() throws Exception {
        List<Map<String,Object>> records = getNestedListOfMaps();
        Map<String,Object> mapDefinition = getReportDefinition();

        redisClient.del(reportName);

        List<Map<String,String>> dimensions = (List<Map<String,String>>) mapDefinition.get("dimensions");
        List<Map<String,String>> metrics = (List<Map<String,String>>) mapDefinition.get("metrics");

        processReport(records, dimensions, metrics);

        // this is cumulative all reports grouped from the begining
        int n = 1000;
        long start = System.currentTimeMillis();
        for (int i=0;i<n;i++) {
            List<Map<String, Object>> report = grouper.getReportList(reportName, dimensions, metrics);
            String s= "";
        }
        long total = System.currentTimeMillis()-start;
        double avg = total/n;
        System.out.println("Processed get full reports size: "+n+" in: "+total+" ms, avg: "+avg+" ms");

    }

}
