package grouper;

import org.datazup.exceptions.EvaluatorException;
import org.datazup.exceptions.GrouperException;
import org.datazup.expression.SelectMapperEvaluator;
import org.datazup.expression.context.ConcurrentExecutionContext;
import org.datazup.grouper.DimensionKey;
import org.datazup.grouper.IGrouper;
import org.datazup.pathextractor.PathExtractor;
import org.datazup.pathextractor.SimpleResolverHelper;
import org.datazup.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * Created by admin@datazup on 3/27/17.
 */

public class GroupMultiLevelSourcesExpressionReportsTest extends TestResourceBase {

    static SimpleResolverHelper mapListResolver = new SimpleResolverHelper();
    static ConcurrentExecutionContext executionContext = new ConcurrentExecutionContext();

    static SelectMapperEvaluator evaluator = SelectMapperEvaluator.getInstance(executionContext, mapListResolver);

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
    public void simpleTest() throws EvaluatorException, GrouperException {
        Map<String,Object> report = loadMapFromResoure("classpath:/report.json");
        List<Map<String,String>> dimensions = (List<Map<String, String>>) report.get("dimensions");
        List<Map<String,String>> metrics = (List<Map<String,String>>) report.get("metrics");

        Assert.assertNotNull(dimensions);
        Assert.assertNotNull(metrics);

        PathExtractor pathExtractor = new PathExtractor(data, new SimpleResolverHelper());
        DimensionKey dimensionKey = new DimensionKey(dimensions, pathExtractor, evaluator);
        dimensionKey.build();
        List<Map<String,Object>> currentMap = grouper.upsert("some:report:name", dimensionKey, metrics);

        Assert.assertNotNull(currentMap);
        Assert.assertTrue(currentMap.size()>0);

        System.out.println("CurrentMap: "+ JsonUtils.getJsonFromObject(currentMap));



    }

}
