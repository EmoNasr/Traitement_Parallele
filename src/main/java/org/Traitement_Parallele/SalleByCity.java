package org.Traitement_Parallele;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class SalleByCity {
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

        JavaPairRDD<String, Double> TraitData = RddSplit
                .mapToPair(CitySales->new Tuple2<String,Double>(CitySales[17],Double.parseDouble(CitySales[4])))
                .reduceByKey((city,sales)->city+sales);

        List<Tuple2<String,Double>> Data = TraitData.collect();
        System.out.println("\n----------------------CitySales-----------------------------------");
        Data.forEach(elm-> System.out.println("City: "+elm._1 + "------ Sales: " + elm._2));
    }
}
