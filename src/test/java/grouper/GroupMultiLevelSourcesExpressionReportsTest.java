package grouper;

import org.datazup.ModuleConfiguration;
import org.datazup.expression.SelectMapperEvaluator;
import org.datazup.grouper.DimensionKey;
import org.datazup.grouper.IGrouper;
import org.datazup.pathextractor.PathExtractor;
import org.datazup.redis.RedisClient;
import org.datazup.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

/**
 * Created by ninel on 3/27/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ModuleConfiguration.class)
public class GroupMultiLevelSourcesExpressionReportsTest {

    static SelectMapperEvaluator evaluator = SelectMapperEvaluator.getInstance();

    private Map<String, Object> data;

    @Autowired
    IGrouper grouper;

    @Autowired
    RedisClient redisClient;

    @Autowired
    ApplicationContext appContext;

    @Before
    public void init() {
        data = loadMapFromResoure("classpath:/data.json");

    }

    private Map<String,Object> loadMapFromResoure(String resourePath){
        String s = loadFromResource(resourePath);
        return JsonUtils.getMapFromJson(s);
    }

    private String loadFromResource(String resourcePath){
        Resource resource =   appContext.getResource(resourcePath);

        StringBuilder result = new StringBuilder("");

        try {
            InputStream is = resource.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            while ((line = br.readLine()) != null) {
                result.append(line);
            }
            br.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return result.toString();
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

        PathExtractor pathExtractor = new PathExtractor(data);
        DimensionKey dimensionKey = new DimensionKey(dimensions, pathExtractor, evaluator);
        dimensionKey.build();
        List<Map<String,Object>> currentMap = grouper.upsert("some:report:name", dimensionKey, metrics);

        Assert.assertNotNull(currentMap);
     ///   Assert.assertTrue(currentMap.size()>0);



    }

}
