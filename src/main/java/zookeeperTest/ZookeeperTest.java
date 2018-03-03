package zookeeperTest;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * For Package zookeeperTest
 * For Project zookeeperTest
 * Created By Administrator
 * Created On 2018/3/3
 */
public class ZookeeperTest {

    private static final int SESSION_TIMEOUT=30000;
    public static final Logger LOGGER= LoggerFactory.getLogger(ZookeeperTest.class);
    private Watcher watcher =new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            LOGGER.info("process :" +watchedEvent.getType());
        }
    };

    private ZooKeeper zooKeeper;

    /**
     * 连接zookeeper
     */


    @Before
    public void connect() throws IOException {
        zooKeeper=new ZooKeeper("192.168.134.141:2181,192.168.137.141:2181",SESSION_TIMEOUT,watcher);
    }

    /**
     * 关闭连接
     */
    @After
    public void close(){
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testCreate(){
        String result=null;
        try {
            result=zooKeeper.create("/zk002","hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            Thread.sleep(30000);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
            Assert.fail();
        }
        LOGGER.info("create result : {}", result);

    }

    @Test
    public void TestDelete(){
        try {
            zooKeeper.delete("/wxm",-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetData(){
        String result=null;
        byte[] bytes = new byte[0];
        try {
            bytes = zooKeeper.getData("/zk002",null,null);
            result=new String(bytes);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
            Assert.fail();
        }
        LOGGER.info("get date: {}",result);

    }

    /**
     * 获取数据，设置watch
     * 时间机制 回调
     *
     */
    @Test
    public void testGetDataWatch(){
        final String result=null;
        System.out.println("get: ");
        try {
            byte[] bytes=zooKeeper.getData("/zk002", new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    LOGGER.info("testGaeDataWatch : {}",watchedEvent.getType());
                    System.out.println("watcher ok");
                    testGetDataWatch();

                }
            },null);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
