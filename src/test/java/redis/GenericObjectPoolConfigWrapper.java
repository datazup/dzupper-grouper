package redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.FactoryBean;

/**
 * Created by admin@datazup on 11/25/16.
 */
public class GenericObjectPoolConfigWrapper implements FactoryBean<GenericObjectPoolConfig> {

    // internal vars ----------------------------------------------------------

    private final GenericObjectPoolConfig config;

    // constructors -----------------------------------------------------------

    public GenericObjectPoolConfigWrapper() {
        this.config = new GenericObjectPoolConfig();
    }

    // getters & setters ------------------------------------------------------

    public GenericObjectPoolConfig getConfig() {
        return config;
    }

    public int getMaxIdle() {
        return this.config.getMaxIdle();
    }

    public void setMaxIdle(int maxIdle) {
        this.config.setMaxIdle(maxIdle);
    }

    public int getMinIdle() {
        return this.config.getMinIdle();
    }

    public void setMinIdle(int minIdle) {
        this.config.setMinIdle(minIdle);
    }

    public int getMaxTotal(){
        return this.config.getMaxTotal();
    }

    public void setMaxTotal(int maxTotal){
        this.config.setMaxTotal(maxTotal);
    }

    public long getMaxWaitMillis() {
        return this.config.getMaxWaitMillis();
    }

    public void setMaxWaitMillis(long maxWait) {
        this.config.setMaxWaitMillis(maxWait);
    }

    public boolean getBlockWhenExhausted() {
        return this.config.getBlockWhenExhausted();
    }

    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.config.setBlockWhenExhausted(blockWhenExhausted);
    }

    public GenericObjectPoolConfig getObject() throws Exception {
        return getConfig();
    }

    public Class<?> getObjectType() {
        // TODO Auto-generated method stub
        return GenericObjectPoolConfig.class;
    }

    public boolean isSingleton() {
        return false;
    }
}