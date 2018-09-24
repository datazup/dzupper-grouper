package base;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * Created by admin@datazup on 11/28/16.
 */

@SpringBootApplication(scanBasePackages = {"org.datazup", "redis"})
@ImportResource("classpath:**/context/redis-context.xml")
public class ModuleConfiguration {
}
