package org.Traitement_Parallele;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDs {
    public static void main(String[] args) {
        List<String> etudiants = Arrays.asList("Etu18","Etu17","Etu16","Etu15","etu14","etu13","etu12","etu10");
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf Sconf = new SparkConf().setAppName("Trai_Paralle").setMaster("local[*]");
        JavaRDD<String> rdd6;
        try (JavaSparkContext sc = new JavaSparkContext(Sconf)) {
            JavaRDD<String> rdd1 = sc.parallelize(etudiants);
            JavaRDD<String> rdd2 = rdd1.map(etu -> {
                return etu.substring(0, 3) + "diant" + etu.substring(3, etu.length());
            });

            JavaRDD<String> rdd3 = rdd2.filter(etu -> Integer.parseInt(etu.substring(etu.length() - 1, etu.length())) % 2 != 0);
            JavaRDD<String> rdd4 = rdd2.filter(etu -> Integer.parseInt(etu.substring(etu.length() - 1, etu.length())) % 2 == 0);
            JavaRDD<String> rdd5 = rdd2.filter(etu -> Integer.parseInt(etu.substring(etu.length() - 2, etu.length())) > 15);

            rdd6 = sc.union(rdd4, rdd5);
        }
        JavaRDD<String> rdd81 = rdd6.map(e->{
            int temp = Integer.parseInt(e.substring(e.length()-1,e.length()))*8;
            return e+=temp;
        });

        JavaPairRDD<String,Integer> rdd8 = rdd81.mapToPair(elt-> new Tuple2<>
                (elt.split("1")[0],
                        Integer.parseInt(elt.split("1")[1]
                        )))
                .reduceByKey((etu,value)->etu+value);

        JavaPairRDD<String, Integer> rdd10 = rdd8.sortByKey();
        List<Tuple2<String,Integer>> listEt = rdd10.collect();
        for (Tuple2<String,Integer> et:listEt) {
            System.out.println(et._1+" "+et._2);
        }
    }
}