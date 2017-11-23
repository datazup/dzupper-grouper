package org.datazup;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * Created by admin@datazup on 11/28/16.
 */
//@Configuration()
//@ComponentScan(basePackages = "org.datazup")
@SpringBootApplication(scanBasePackages = "org.datazup")
@ImportResource("classpath:context/redis-context.xml")
public class ModuleConfiguration {
}
