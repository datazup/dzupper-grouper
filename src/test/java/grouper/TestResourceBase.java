package grouper;

import org.datazup.ModuleConfiguration;
import org.datazup.utils.JsonUtils;
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
import java.util.Map;

/**
 * Created by ninel on 3/29/17.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ModuleConfiguration.class)
public abstract class TestResourceBase {


    @Autowired
    ApplicationContext appContext;

    protected Map<String,Object> loadMapFromResoure(String resourePath){
        String s = loadFromResource(resourePath);
        return JsonUtils.getMapFromJson(s);
    }

    protected String loadFromResource(String resourcePath){
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
}
