package org.Traitement_Parallele;

import org.apache.hadoop.security.SaslOutputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.List;

public class MeteoAnaly2020 {

    public static void main(String[] args) {


        SparkConf Sconf = new SparkConf()
                .setAppName("MeteoAnaly")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(Sconf);
        JavaRDD<String> data = sc.textFile("src/2020.csv");

        JavaRDD<String[]> splitedData = data.map(e->e.split(","));

        //Element with indicator TMIN
        JavaRDD<Double> Tmin = splitedData.filter(e->e[2].equals("TMIN"))
                .map(e->Double.parseDouble(e[3]));

        //Element with indicator TMAX
        JavaRDD<Double> Tmax = splitedData.filter(e->e[2].equals("TMAX"))
                .map(e->Double.parseDouble(e[3]));

        //Average temperature min and max
        double sommeTmin = Tmin.reduce((a,b)->a+b);
        double sommeTmax = Tmax.reduce((a,b)->a+b);
        double NbTmin = Tmin.count();
        double NbTmax = Tmax.count();

        //Max temperature and Min temperature
        double maxT = splitedData
                .map(e->Double.parseDouble(e[3]))
                .reduce((t1,t2)->Math.max(t1,t2));

        double minT = splitedData
                .map(e->Double.parseDouble(e[3]))
                .reduce((t1,t2)->Math.min(t1,t2));

        System.out.println("Average Temperature MIN: "+sommeTmin/NbTmin);
        System.out.println("Average Temperature MAX: "+sommeTmax/NbTmax);
        System.out.println("Temperature MAX: "+maxT);
        System.out.println("Temperature MIN: "+minT);




    }

}
