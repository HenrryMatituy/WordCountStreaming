package flink;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;

import java.io.UnsupportedEncodingException;

import java.time.Duration;

import static org.apache.jena.riot.RDFLanguages.strLangNTriples;

public class WordCount {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<Triple> text;

        text = LoadTriples.fromDataset(env, params.get("input"));

        text.print();


        env.execute("Streaming WordCount");
    }

}

class LoadTriples {
    public static DataStream<Triple> fromDataset(StreamExecutionEnvironment environment, String filePath) {
        Preconditions.checkNotNull(filePath, "The file path may not be null.");

        DataStreamSource<String> dataStreamSource = environment.readTextFile(filePath);
        DataStream<Triple> datastream = dataStreamSource.map(new String2Triple());

        return datastream;
    }
}

class String2Triple implements MapFunction<String, Triple> {

    @Override
    public Triple map(String line){
        Model inputModel = ModelFactory.createDefaultModel();
        try {
            // Load model with arg string (expecting n-triples)
            inputModel = inputModel.read(new StringInputStream(line), null, strLangNTriples);

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);

        }
        Statement stmt = inputModel.listStatements().nextStatement();
        return stmt.asTriple();
    }
}