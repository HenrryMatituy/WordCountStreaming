package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingWordCount {

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("USO:\nbatStreamingWordCount <host> <puerto> <ruta_salida>");
            return;
        }

        String host = args[0];
        Integer puerto = Integer.parseInt(args[1]);
        String rutaSalida = args[2];

        // instanciamos el entorno de ejecución
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // obtenemos el stream de datos del host provisto
        DataStream<String> text = env.socketTextStream("localhost", 5555);

        DataStream<Tuple2<String, Integer>> conteo =
                // realizamos un mapeo de las palabras a tuplas de la forma (palabra, 1)
                text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                            public void flatMap(String valor, Collector<Tuple2<String, Integer>> salida) throws Exception {
                                String[] palabras = valor.toLowerCase().split("\\W+");

                                for (String palabra : palabras) {
                                    if (palabra.isEmpty() == false) salida.collect(new Tuple2<String, Integer>(palabra, 1));
                                }
                            }
                        })
                        // utilizamos la palabra (posición 0) como clave, y sumamos las frecuencias (posición 1)
                        .keyBy(0).sum(1);

        conteo.print();

        // execute program
        env.execute("WordCount en Streaming");
    }
}