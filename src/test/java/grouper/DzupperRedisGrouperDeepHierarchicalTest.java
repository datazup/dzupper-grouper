package grouper;

import base.TestBase;
import org.datazup.ModuleConfiguration;
import org.datazup.expression.SelectMapperEvaluator;
import org.datazup.grouper.DimensionKey;
import org.datazup.grouper.IGrouper;
import org.datazup.pathextractor.PathExtractor;
import org.datazup.redis.RedisClient;
import org.datazup.utils.JsonUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

/**
 * Created by ninel on 12/6/16.
 */

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ModuleConfiguration.class)
public class DzupperRedisGrouperDeepHierarchicalTest extends TestBase {

    @Autowired
    IGrouper grouper;

    @Autowired
    RedisClient redisClient;

    String reportName = "ReportDeepHieararchical_company:custom:ReportDeepHieararchical";

    static SelectMapperEvaluator evaluator = SelectMapperEvaluator.getInstance();

    private Map<String,Object> getReportDefinition(){

        String newDef = "{"
                +"\"dimensions\":[{\"name\":\"$child[].name1$\", \"type\":\"string\"}, {\"name\":\"$child[].type1$\", \"type\":\"string\"},"
                +"{\"name\":\"HOUR($date$)\", \"type\":\"int\"},{\"name\":\"DAY($date$)\", \"type\":\"int\"},{\"name\":\"MONTH($date$)\", \"type\":\"int\"},{\"name\":\"YEAR($date$)\", \"type\":\"int\"}],"

                +"\"metrics\":[{\"name\":\"SUM($amount$)\", \"type\":\"int\"},{\"name\":\"COUNT($amount$)\", \"type\":\"int\"}] "
                +"}";
        Map<String,Object> mapDefinition = JsonUtils.getMapFromJson(newDef);
        return mapDefinition;
    }

    private void processReport(List<Map<String,Object>> records, List<Map<String,String>> dimensions, List<Map<String,String>> metrics){
        long start = System.currentTimeMillis();
        for (Map<String,Object> streamMap: records){
            PathExtractor pathExtractor = new PathExtractor(streamMap);
            DimensionKey dimensionKey = new DimensionKey(dimensions, pathExtractor, evaluator);
            dimensionKey.build();
            List<Map<String,Object>> currentMap = grouper.upsert(reportName, dimensionKey, metrics);
            System.out.println("CurrentMap: "+ JsonUtils.getJsonFromObject(currentMap));
           /* Assert.assertTrue(currentMap.size()==8);
            Assert.assertTrue(currentMap.containsKey("SUMamount"));
            Assert.assertTrue(currentMap.containsKey("COUNTamount"));*/
            System.out.println("CurrentMap: "+ JsonUtils.getJsonFromObject(currentMap));
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
        }
        long total = System.currentTimeMillis()-start;
        double avg = total/n;
        System.out.println("Processed get full reports size: "+n+" in: "+total+" ms, avg: "+avg+" ms");

    }
}
