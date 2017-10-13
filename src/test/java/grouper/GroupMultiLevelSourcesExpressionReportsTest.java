package grouper;

import org.datazup.expression.SelectMapperEvaluator;
import org.datazup.grouper.DimensionKey;
import org.datazup.grouper.IGrouper;
import org.datazup.pathextractor.PathExtractor;
import org.datazup.pathextractor.SimpleMapListResolver;
import org.datazup.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * Created by ninel on 3/27/17.
 */

public class GroupMultiLevelSourcesExpressionReportsTest extends TestResourceBase {

    static SelectMapperEvaluator evaluator = SelectMapperEvaluator.getInstance();

    private Map<String, Object> data;

    @Autowired
    IGrouper grouper;

    @Before
    public void init() {
        data = loadMapFromResoure("classpath:/hierarchicalData.json");

    }
    @Test
    public void isDataLoadedTest(){
        Assert.assertNotNull(data);
    }

    @Test
    public void simpleTest(){
        Map<String,Object> report = loadMapFromResoure("classpath:/report.json");
        List<Map<String,String>> dimensions = (List<Map<String, String>>) report.get("dimensions");
        List<Map<String,String>> metrics = (List<Map<String,String>>) report.get("metrics");

        Assert.assertNotNull(dimensions);
        Assert.assertNotNull(metrics);

        PathExtractor pathExtractor = new PathExtractor(data, new SimpleMapListResolver());
        DimensionKey dimensionKey = new DimensionKey(dimensions, pathExtractor, evaluator);
        dimensionKey.build();
        List<Map<String,Object>> currentMap = grouper.upsert("some:report:name", dimensionKey, metrics);

        Assert.assertNotNull(currentMap);
        Assert.assertTrue(currentMap.size()>0);

        System.out.println("CurrentMap: "+ JsonUtils.getJsonFromObject(currentMap));



    }

}
