import org.junit.Test;
import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.HashMap;

/**
 * @author yinlei
 * @since 2018/9/28 14:13
 */
public class UseScalaInJavaTest {
    @Test
    public void test() {
        Map<String, String> map = new HashMap<>();
        map = map.$plus(new Tuple2<>("password", "123456"));
        map = map.$plus(new Tuple2<>("user", "root"));
        map = map.$plus(new Tuple2<>("hbase.zookeepers", "server1"));
        map = map.$plus(new Tuple2<>("hbase.zookeeper.quorum", "server1,server2,server3"));
        map = map.$plus(new Tuple2<>("hbase.catalog", "t1"));
        map = map.$plus(new Tuple2<>("tableName", "t1"));

        System.out.println(map);
    }
}
