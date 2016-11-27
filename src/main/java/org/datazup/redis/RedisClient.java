package org.datazup.redis;

import org.datazup.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.io.Serializable;
import java.util.*;

/**
 * Created by ninel on 11/25/16.
 */
public class RedisClient  implements Serializable {


    private static final long serialVersionUID = -6780499569379661348L;
    private static Logger LOG = LoggerFactory.getLogger(RedisClient.class);

    protected transient JedisPool jedisPool;

    private int DEFAULT_POP_TIMEOUT = 1;

    public RedisClient(JedisPool jedisPool) throws Exception {

        this.jedisPool = jedisPool;

        if (LOG.isInfoEnabled()) {
            String pong = ping();
            LOG.info("We ping the redis and it returns: " + pong);
        }
    }

    private String ping() throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.ping();
        } catch (Exception e) {
            // jedisPool.returnBrokenResource(jedis);
            jedis.close();
            throw new Exception("Redis server is not accessible", e);
        } finally {
            // jedisPool.returnResource(jedis);
            jedis.close();
        }
    }

    public void set(String key, int ttl, String data) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.setex(key, ttl, data);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in cache set for key: " + key + " and data: " + data, e);
        } finally {
            jedis.close();
        }

    }

    public String get(String key) throws Exception {
        Jedis jedis = jedisPool.getResource();
        String val = null;
        try {
            if (jedis.exists(key))
                val = jedis.get(key);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in cache set for key: " + key, e);
        } finally {
            jedis.close();
        }
        return val;
    }

    public void destroy() {
        try {
            if (null != jedisPool) {
                jedisPool.close();
                // jedisPool.destroy();
            }
            LOG.warn("DESTROY - close is called");
        } catch (Exception e) {
            LOG.error("Error closing REDIS instance", e);
        }
    }

    public void sadd(String key, Integer ttl, String val) throws Exception {
        Jedis jedis = jedisPool.getResource();

        try {

            Pipeline pl = jedis.pipelined();
            pl.sadd(key, val);
            pl.expire(key, ttl);
            pl.sync();

        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in cache Add to Set for key: " + key + " and value: " + val, e);
        } finally {
            jedis.close();
        }

    }

    public void srem(String key, String val) throws Exception {

        Jedis jedis = jedisPool.getResource();
        try {
            if (null == val) {
                // we need to remove all items in a set and the key as well
                jedis.del(key);
            } else {
                jedis.srem(key, val);
            }
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in cache set for key: " + key + " and value: " + val, e);
        } finally {
            jedis.close();
        }
    }

    public String[] smembers(String key) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            Set<String> members = jedis.smembers(key);
            return members.toArray(new String[members.size()]);

        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in cache while getting set members for key: " + key, e);
        } finally {
            jedis.close();
        }
    }

    public List<String> dequeueStream(String namespace) throws Exception {

        Jedis jedis = jedisPool.getResource();
        try {
            String result = jedis.lpop(namespace);
            return new ArrayList<String>(Arrays.asList(result));
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in dequeue of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public synchronized List<String> dequeue(String namespace, Integer timeout) throws Exception {

        if (null == timeout) {
            timeout = DEFAULT_POP_TIMEOUT;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            List<String> result = jedis.blpop(timeout, namespace);
            return result;
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in dequeue of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }

    }

    public synchronized void enqueue(String namespace, String... values) throws Exception {

        Jedis jedis = jedisPool.getResource();
        try {
            jedis.rpush(namespace, values);
        } catch (Exception e) {
            jedis.close();
            throw new Exception(
                    "Error in enqueue of namespace: " + namespace + " for array: " + Arrays.deepToString(values), e);
        } finally {
            jedis.close();
        }
    }

    public String addToHash(String namespace, Map<String, String> map) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hmset(namespace, map);
        } catch (Exception e) {
            jedis.close();
            throw new Exception(
                    "Error in hmset of namespace: " + namespace + " for array: " + JsonUtils.getJsonFromObject(map), e);
        } finally {
            jedis.close();
        }
    }

    public Map<String, String> getFromHash(String namespace) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hgetAll(namespace);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in hgetAll of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Map<String, String> getFromHash(String namespace, String... fields) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            List<String> listOfKeyValPairs = jedis.hmget(namespace, fields);
            Map<String, String> map = null;
            if (null != listOfKeyValPairs) {
                map = new HashMap<String, String>();
                for (String keyVal : listOfKeyValPairs) {
                    String[] keyValSplit = keyVal.split(",");
                    map.put(keyValSplit[0], keyValSplit[1]);
                }
            }
            return map;
        } catch (Exception e) {
            jedis.close();
            throw new Exception(
                    "Error in hmget of namespace: " + namespace + " and fields: " + Arrays.deepToString(fields), e);
        } finally {
            jedis.close();
        }
    }

    public String getFromHash(String namespace, String field) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hget(namespace, field);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in hget of namespace: " + namespace + " and field: " + field, e);
        } finally {
            jedis.close();
        }
    }

    public Long increment(String namespace) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.incr(namespace);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in incr of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Long expire(String namespace, int ttl) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.expire(namespace, ttl);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in expire of namespace: " + namespace + " for ttl: " + ttl, e);
        } finally {
            jedis.close();
        }
    }

    public Long incrementHashFieldByValue(String namespace, String field, Long val) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hincrBy(namespace, field, val);
        } catch (Exception e) {
            jedis.close();
            throw new Exception(
                    "Error in hincrBy of namespace: " + namespace + " for field: " + field + " and value: " + val, e);
        } finally {
            jedis.close();
        }
    }

    public Double incrementHashFieldByFloatValue(String namespace, String field, double val) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hincrByFloat(namespace, field, val);
        } catch (Exception e) {
            jedis.close();
            throw new Exception(
                    "Error in hincrBy of namespace: " + namespace + " for field: " + field + " and value: " + val, e);
        } finally {
            jedis.close();
        }
    }

    public Long addToHash(String namespace, String field, String val) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hset(namespace, field, val);
        } catch (Exception e) {
            jedis.close();
            throw new Exception(
                    "Error in hset of namespace: " + namespace + " for field: " + field + " and value: " + val, e);
        } finally {
            jedis.close();
        }

    }

    public Long getTtl(String namespace) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.ttl(namespace);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Boolean sIsMember(String namespace, String value) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.sismember(namespace, value);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public void addToSortedSet(String namespace, double score, String value) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.zadd(namespace, score, value);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public void remFromSortedSet(String namespace, String... value) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.zrem(namespace, value);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Set<Tuple> getSortedSetByRange(String namespace, Integer start, Integer end) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {

            Set<Tuple> tuple = jedis.zrangeWithScores(namespace, start, end);

            return tuple; // jedis.zrange(namespace, start, end);

        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Set<String> getSortedSetByRangeScore(String namespace, Double min, Double max, Integer offset, Integer count)
            throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {

            String strMin = "-inf";
            String strMax = "+inf";

            if (null != min)
                strMin = min.toString();

            if (null != max)
                strMax = max.toString();

            if (null == offset || null == count)
                return jedis.zrangeByScore(namespace, strMin, strMax);

            return jedis.zrangeByScore(namespace, strMin, strMax, offset, count);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Boolean hexists(String namespace, String fieldName) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {

            return jedis.hexists(namespace, fieldName);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Boolean exists(String namespace) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {

            return jedis.exists(namespace);
        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public Set<Tuple> getSortedSetByRevRange(String namespace, Integer start, Integer end) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {

            Set<Tuple> tuple = jedis.zrevrangeWithScores(namespace, start, end);
            return tuple; // jedis.zrange(namespace, start, end);

        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in ttl of namespace: " + namespace, e);
        } finally {
            jedis.close();
        }
    }

    public void del(String namespace) throws Exception {
        Jedis jedis = jedisPool.getResource();
        try {

            jedis.del(namespace);

        } catch (Exception e) {
            jedis.close();
            throw new Exception("Error in deleting the key: " + namespace, e);
        } finally {
            jedis.close();
        }
    }
}
