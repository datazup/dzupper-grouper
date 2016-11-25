package org.datazup;

import org.datazup.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ImportResource;

/**
 * Created by ninel on 11/25/16.
 */

@SpringBootApplication(scanBasePackages = "org.datazup")
@ImportResource("classpath:context/redis-context.xml")
public class Application {

    private static Logger LOG = null;

    @Autowired
    RedisClient redisClient;

    public static void main(String[] args){

        System.setProperty("log_name", Application.class.getSimpleName());
        LOG = LoggerFactory.getLogger(Application.class);

        ConfigurableApplicationContext context = SpringApplication.run(Application.class,
                args);

        Application app = context.getBean(Application.class);
        app.run();
    }

    private void run() {
        try {
            String v = redisClient.get("");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
