package wordcount;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount implements java.io.Serializable {
	private static final long serialVersionUID = 1L;

  public void run(String path) {
	if( new File(path).exists() == false ){
		throw new RuntimeException("File doesnt exist");
	}

	String master = "local[*]";

	SparkConf conf = new SparkConf()
		.setAppName(WordCount.class.getName())
		.setMaster(master);
		
	JavaSparkContext context = new JavaSparkContext(conf);

	List<Tuple2<String, Integer>> results = context.textFile(path)
		.flatMap(new FlatMapSplitText())
		.mapToPair(new PairFunctionCountOfOne())
		.reduceByKey(new AdditionFunction())
		.collect();

	for(Tuple2<String, Integer> result : results) {
		System.out.println(result._1 + "=" + result._2);
	}

	context.close();
  }
	
  class FlatMapSplitText implements FlatMapFunction<String, String>{
	private static final long serialVersionUID = 1L;

	public Iterator<String> call(String txt) throws Exception {
			return Arrays.asList(txt.split(" ")).iterator();
		}
	}
  
	class PairFunctionCountOfOne implements PairFunction<String, String, Integer> {
		private static final long serialVersionUID = 1L;

		public Tuple2<String, Integer> call(String txt) throws Exception {
			return new Tuple2<String, Integer>(txt, 1);
		}
	}
  
	class AdditionFunction implements Function2<Integer, Integer, Integer> {
		private static final long serialVersionUID = 1L;
		
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	}
}
