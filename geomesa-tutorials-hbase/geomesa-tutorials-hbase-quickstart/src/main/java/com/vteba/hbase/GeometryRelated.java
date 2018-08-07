package com.vteba.hbase;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * gemotry之间的关系，支持的空间操作包括：
 * <p>相等(Equals)：几何形状拓扑上相等。</p>
 * <p>脱节(Disjoint)：几何形状没有共有的点。</p>
 * <p>相交(Intersects)：几何形状至少有一个共有点（区别于脱节）</p>
 * <p>接触(Touches)：几何形状有至少一个公共的边界点，但是没有内部点。</p>
 * <p>交叉(Crosses)：几何形状共享一些但不是所有的内部点。</p>
 * <p>内含(Within)：几何形状A的线都在几何形状B内部。</p>
 * <p>包含(Contains)：几何形状B的线都在几何形状A内部（区别于内含）</p>
 * <p>重叠(Overlaps)：几何形状共享一部分但不是所有的公共点，而且相交处有他们自己相同的区域。</p>
 *
 * @author yinlei
 * @since 2018/8/7 15:12
 */
public class GeometryRelated {

    private GeometryFactory geometryFactory = new GeometryFactory();

    /**
     *  两个几何对象是否是重叠的
     * @return
     * @throws ParseException
     */
    public boolean equalsGeo() throws ParseException{
        WKTReader reader = new WKTReader( geometryFactory );
        LineString geometry1 = (LineString) reader.read("LINESTRING(0 0, 2 0, 5 0)");
        LineString geometry2 = (LineString) reader.read("LINESTRING(5 0, 0 0)");
        return geometry1.equals(geometry2);//true
    }

    /**
     * 几何对象没有交点(相邻)
     * @return
     * @throws ParseException
     */
    public boolean disjointGeo() throws ParseException{
        WKTReader reader = new WKTReader( geometryFactory );
        LineString geometry1 = (LineString) reader.read("LINESTRING(0 0, 2 0, 5 0)");
        LineString geometry2 = (LineString) reader.read("LINESTRING(0 1, 0 2)");
        return geometry1.disjoint(geometry2);
    }

    /**
     * 至少一个公共点(相交)
     * @return
     * @throws ParseException
     */
    public boolean intersectsGeo() throws ParseException{
        WKTReader reader = new WKTReader( geometryFactory );
        LineString geometry1 = (LineString) reader.read("LINESTRING(0 0, 2 0, 5 0)");
        LineString geometry2 = (LineString) reader.read("LINESTRING(0 0, 0 2)");
        Geometry interPoint = geometry1.intersection(geometry2);//相交点
        System.out.println(interPoint.toText());//输出 POINT (0 0)
        return geometry1.intersects(geometry2);
    }

    /**
     * 判断以x,y为坐标的点point(x,y)是否在geometry表示的Polygon中
     * @param x
     * @param y
     * @param geometry wkt格式
     * @return
     */
    public boolean withinGeo(double x,double y,String geometry) throws ParseException {

        Coordinate coord = new Coordinate(x,y);
        Point point = geometryFactory.createPoint( coord );

        WKTReader reader = new WKTReader( geometryFactory );
        Polygon polygon = (Polygon) reader.read(geometry);
        return point.within(polygon);
    }
    /**
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException {
        GeometryRelated gr = new GeometryRelated();
        System.out.println(gr.equalsGeo());
        System.out.println(gr.disjointGeo());
        System.out.println(gr.intersectsGeo());
        System.out.println(gr.withinGeo(5,5,"POLYGON((0 0, 10 0, 10 10, 0 10,0 0))"));
    }

}
