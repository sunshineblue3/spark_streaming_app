package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Serializable;
import scala.Tuple2;
import java.util.Arrays;


public class WordCount implements Serializable {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("WordCount")
                .set("spark.serializer", KryoSerializer.class.getName());

//      Интервал пакетной обработки
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(500));

//      Определение сокета, который будет прослушиваться
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

//      Разбиваем строку на слова
//      flatMap применяет функцию к DStream, может возвращать несколько значений
        JavaDStream<String> words = lines.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator()
        );

//      Подсчитываем каждое слово в каждом пакете
//      mapToPair преобразует в пару значений - здесь создается пара ключ-значение,
//      где ключ - слово, значение - количество повторений - по дефолту 1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1)
        );

//      Суммируем по ключу
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                (i1, i2) -> i1 + i2
        );

//      Вывод на консоль
        lines.print();

        jssc.start();

        jssc.awaitTermination();

        jssc.stop();
    }

}
