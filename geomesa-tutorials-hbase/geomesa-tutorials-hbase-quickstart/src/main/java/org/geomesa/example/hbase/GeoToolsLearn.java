package org.geomesa.example.hbase;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.geotools.geometry.jts.JTSFactoryFinder;

/**
 * @author yinlei
 * @since 2018/7/30 15:28
 */
public class GeoToolsLearn {
    public static void main(String[] args) throws Exception {
        // 一个点包含在一个多边形内
        String wktPoly = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"; //请自行搜素了解wkt格式
        String wktPoint = "POINT (30 30)";
        WKTReader reader = new WKTReader(JTSFactoryFinder.getGeometryFactory());
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        Geometry point = reader.read(wktPoint);
        Geometry poly = reader.read(wktPoly);
        System.out.println(poly.contains(point)); //返回true或false

        test();
    }

    static void test() throws Exception {
        String wktPoint = "POINT(103.83489981581 33.462715497945)";
        String wktLine = "LINESTRING(108.32803893589 41.306670233001,99.950999898452 25.84722546391)";
        String wktPolygon = "POLYGON((100.02715479879 32.168082192159,102.76873121104 37.194305614622,107.0334056301 34.909658604412,105.96723702534 30.949603786713,100.02715479879 32.168082192159))";
        String wktPolygon1 = "POLYGON((96.219409781775 32.777321394882,96.219409781775 40.240501628236,104.82491352023001 40.240501628236,104.82491352023001 32.777321394882,96.219409781775 32.777321394882))";

        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory( null );
        WKTReader reader = new WKTReader( geometryFactory );
        Point point = (Point) reader.read(wktPoint);
        LineString line = (LineString) reader.read(wktLine);
        Polygon polygon = (Polygon) reader.read(wktPolygon);
        Polygon polygon1 = (Polygon) reader.read(wktPolygon1);
        System.out.println("-------空间关系判断-------");
        System.out.println(polygon.contains(point));
        System.out.println(polygon.intersects(line));
        System.out.println(polygon.overlaps(polygon1));

        System.out.println("\r\n-------空间计算-------");
        WKTWriter write = new WKTWriter();
        Geometry intersection = polygon.union( polygon1 );
        Geometry union = polygon.union( polygon1 );
        Geometry difference = polygon.difference( polygon1 );
        Geometry symdifference = polygon.symDifference( polygon1 );
        System.out.println("\t+++++++++++叠加分析+++++++++++");
        System.out.println(write.write(intersection));
        System.out.println("\t+++++++++++合并分析+++++++++++");
        System.out.println(write.write(union));
        System.out.println("\t+++++++++++差异分析+++++++++++");
        System.out.println(write.write(difference));
        System.out.println("\t+++++++++++sym差异分析+++++++++++");
        System.out.println(write.write(symdifference));


    }
}
