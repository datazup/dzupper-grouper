package grouper;

import base.TestBase;
import org.datazup.Application;
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
 * Created by ninel on 11/25/16.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class DzupperGrouperTest extends TestBase {

    @Autowired
    RedisClient redisClient;

    @Test
    public void izDzupperGroupingTheData() throws Exception {

        List<Map<String,Object>> records = getNestedListOfMaps();

        String def = "{"
                +"\"dimensions\":[\"$name$\", \"$type$\", HOUR(\"$date$\"), DAY(\"$date$\"), MONTH(\"$date$\"), YEAR(\"$date$\")], "
                +"\"metrics\":[\"SUM($amount$)\", \"COUNT($amount$)\"] "
                +"}";

        Map<String,Object> mapDefinition = JsonUtils.getMapFromJson(def);

        for (Map<String,Object> streamMap: records){

        }

    }

}
