package org.Traitement_Parallele;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class SalesByCityAndYear {
    public static void main(String[] args) {
        SparkConf Sconf = new SparkConf()
                .setAppName("SalesByCity")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(Sconf);
        JavaRDD<String> dataWithHeader = sc.textFile("src/sales_data_sample.csv");
        //To delete the header of the csv file
        JavaRDD<String> dataWithoutHeader = dataWithHeader.filter(lines->!lines.equals(dataWithHeader.first()));
        String header = (dataWithHeader.first());

        JavaRDD<String[]> RddSplit = dataWithHeader
                .filter(line->!line.equals(header))
                .map(line->line.split(","));

        //Sales by the year 2003 for each city
        String y = "2003";
        JavaPairRDD<String, Double> TraitData = RddSplit
                .filter(year->year[9].equals(y))
                .mapToPair(CitySales->new Tuple2<String,Double>(CitySales[17],Double.parseDouble(CitySales[4])))
                .reduceByKey((city,sales)->city+sales);

        List<Tuple2<String,Double>> Data = TraitData.collect();
        System.out.println("\n----------------------CitySales "+y+" -----------------------------------");
        Data.forEach(elm-> System.out.println("City: "+elm._1 + "------ Sales: " + elm._2));
    }
}
