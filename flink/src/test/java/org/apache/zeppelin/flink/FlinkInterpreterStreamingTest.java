/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.flink;


import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FlinkInterpreterStreamingTest {

  private static FlinkInterpreterStreaming flink;
  private static InterpreterContext context;

  @BeforeClass
  public static void setUp() {
    Properties p = new Properties();
    flink = new FlinkInterpreterStreaming(p);
    flink.open();
    context = new InterpreterContext(null, null, null, null, null, null, null, null);
  }

  @AfterClass
  public static void tearDown() {
    flink.close();
    flink.destroy();
  }

  @Test
  public void testSimpleStatement() {
    InterpreterResult result = flink.interpret("val a=1", context);
    result = flink.interpret("print(a)", context);
    assertEquals("1", result.message());
  }


  @Test
  public void testImmidiateHasNext() {


    PrintStream outOld = System.out;


//    flink.interpret("import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction",context);
//    flink.interpret("import org.apache.flink.api.scala._",context);
//
//    flink.interpret("import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction}",context);
//    flink.interpret("import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment",context);
//    flink.interpret("import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream}",context);
//    flink.interpret("import org.apache.flink.streaming.connectors.rabbitmq.RMQSource",context);
//    flink.interpret("import org.apache.flink.streaming.util.serialization.SimpleStringSchema",context);
//    flink.interpret("import org.apache.flink.util.Collector",context);
//    flink.interpret("import org.json.simple.parser.JSONParser",context);
//    flink.interpret("import org.json.simple.JSONObject",context);
//    flink.interpret("import org.json.simple.JSONArray",context);
//    flink.interpret("import org.apache.flink.contrib.streaming.scala.DataStreamUtils._",context);
//
//    flink.interpret("var data: DataStream[String] = env.addSource(new RMQSource[String](\"localhost\", \"DATA\", new SimpleStringSchema)) //FLINK_DATA",context);
//
//    flink.interpret("val feedback_in: DataStream[String] = env.addSource(new RMQSource[String](\"localhost\", \"mikeQueue\", new SimpleStringSchema))",context);
//
//    flink.interpret("val feedback: DataStream[JSONObject] = feedback_in.map{",context);
//      flink.interpret("new MapFunction[String,JSONObject]{",context);
//        flink.interpret("override def map(t: String): JSONObject = {",context);
//                flink.interpret("val parser: JSONParser = new JSONParser",context);
//        flink.interpret("val elem = parser.parse(t).asInstanceOf[JSONObject]",context);
//                flink.interpret("return(elem)",context);
//                flink.interpret("}",context);
//        flink.interpret("}",context);
//      flink.interpret("}",context);
//
//    // downsample data:
//
//    flink.interpret("var dataConnected: ConnectedStreams[String, JSONObject] = data.connect(feedback);",context);
//    flink.interpret("val tablerows: DataStream[String] = dataConnected.flatMap {",context);
//      flink.interpret("  new CoFlatMapFunction[String, JSONObject, String] {",context);
//
//        // downsample parameters
//        flink.interpret("var passProbability: Double = 1.0",context);
//
//        // filter by attribute
//        flink.interpret("var neLat: Double = 0.0",context);
//        flink.interpret("var neLng: Double = 0.0",context);
//        flink.interpret("var swLat: Double = 0.0",context);
//        flink.interpret("var swLng: Double = 0.0",context);
//
//        flink.interpret("override def flatMap2(elem: JSONObject, collector: Collector[String]): Unit = {",context);
//
//          flink.interpret("val control = elem.get(\"control\").asInstanceOf[JSONObject]",context);
//
//          flink.interpret("val bounds = control.get(\"bounds\").asInstanceOf[JSONObject]",context);
//
//          flink.interpret("val northEast = bounds.get(\"_northEast\").asInstanceOf[JSONObject]",context);
//          flink.interpret("val southWest = bounds.get(\"_southWest\").asInstanceOf[JSONObject]",context);
//
//          flink.interpret("neLat = northEast.get(\"lat\").asInstanceOf[Double]",context);
//          flink.interpret("neLng = northEast.get(\"lng\").asInstanceOf[Double]",context);
//
//          flink.interpret("swLat = southWest.get(\"lat\").asInstanceOf[Double]",context);
//          flink.interpret("swLng = southWest.get(\"lng\").asInstanceOf[Double]",context);
//
//          flink.interpret("var items = elem.get(\"num_items\").asInstanceOf[Integer]",context);
//
//          flink.interpret("println(\"got control message!\")",context);
//          flink.interpret("println(\"nelat:\"+ neLat + \"neLng:\" + neLng + \"swLat:\" + swLat + \"swLng:\" + swLng)",context);
//          flink.interpret("}",context);
//
//        flink.interpret("override def flatMap1(in: String, collector: Collector[String]): Unit = {",context);
//
//        flink.interpret("        val parser: JSONParser = new JSONParser",context);
//
//        flink.interpret("val elem = parser.parse(in).asInstanceOf[JSONObject]",context);
//        flink.interpret("var resString = \"\"",context);
//
//        flink.interpret("var entities = elem.get(\"entities\").asInstanceOf[JSONObject]",context);
//        flink.interpret("var hashtags = entities.get(\"hashtags\").asInstanceOf[JSONArray]",context);
//
//        flink.interpret("if (hashtags.size() > 0) {",context);
//          flink.interpret("val hashtag: JSONObject = hashtags.get(0).asInstanceOf[JSONObject]",context);
//          flink.interpret("val hashtagtext: String = hashtag.get(\"text\").asInstanceOf[String]",context);
//          flink.interpret("resString = hashtagtext",context);
//        flink.interpret("} else {",context);
//          flink.interpret("resString = \"hello\"",context);
//        flink.interpret("}",context);
//
//        //fill location statistics
//        flink.interpret("val coordinates = elem.get(\"coordinates\").asInstanceOf[JSONObject]",context);
//        flink.interpret("if (coordinates != null) {",context);
//          flink.interpret("val coordinates1: JSONArray = coordinates.get(\"coordinates\").asInstanceOf[JSONArray]",context);
//
//          // TODO: sometin's messed up yo!
//          flink.interpret("val lng: Double = coordinates1.get(0).asInstanceOf[Double]",context);
//          flink.interpret("val lat: Double = coordinates1.get(1).asInstanceOf[Double]",context);
//
//          flink.interpret("println(\"collecting:\" + lat + \" and \" + lng)",context);
//          flink.interpret("println(neLat + \",\" + neLng + \"; \" + swLat + \",\" + swLng)",context);
//          // if its inside of boundingbox:
//          flink.interpret("if (lat < neLat && lat > swLat && lng < neLng && lng > swLng) {",context);
//            flink.interpret("collector.collect(resString + \"\t\" + lat + \"\t\" + lng + \"\n\")",context);
//            flink.interpret("}",context);
//          flink.interpret("}",context);
//        flink.interpret("}",context);
//        flink.interpret("}",context);
//      flink.interpret("}",context);
//
//
//
//
//    flink.interpret("var it = collect(tablerows)",context);

        String flinkProgram = "import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction\n" +
        "import org.apache.flink.api.scala._\n" +
        "\n" +
        "import org.apache.flink.api.common.functions.{MapFunction, FlatMapFunction}\n" +
        "import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment\n" +
        "import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream}\n" +
        "import org.apache.flink.streaming.connectors.rabbitmq.RMQSource\n" +
        "import org.apache.flink.streaming.util.serialization.SimpleStringSchema\n" +
        "import org.apache.flink.util.Collector\n" +
        "import org.json.simple.parser.JSONParser\n" +
        "import org.json.simple.JSONObject\n" +
        "import org.json.simple.JSONArray\n" +
        "import org.apache.flink.contrib.streaming.scala.DataStreamUtils._\n" +
        "import org.apache.flink.contrib.streaming.java.DataStreamIterator\n" +
        "\n" +
        "\n" +
        "\n" +
        "\n" +
        "var data: DataStream[String] = env.addSource(new RMQSource[String](\"localhost\", \"DATA\", new SimpleStringSchema)) //FLINK_DATA\n" +
        "\n" +
        "    val feedback_in: DataStream[String] = env.addSource(new RMQSource[String](\"localhost\", \"mikeQueue\", new SimpleStringSchema))\n" +
        "\n" +
        "      val feedback: DataStream[JSONObject] = feedback_in.map{\n" +
        "        new MapFunction[String,JSONObject]{\n" +
        "          override def map(t: String): JSONObject = {\n" +
        "            val parser: JSONParser = new JSONParser\n" +
        "            val elem = parser.parse(t).asInstanceOf[JSONObject]\n" +
        "            return(elem)\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "\n" +
        "    // downsample data:\n" +
        "\n" +
        "    var dataConnected: ConnectedStreams[String, JSONObject] = data.connect(feedback);\n" +
        "    val tablerows: DataStream[String] = dataConnected.flatMap {\n" +
        "      new CoFlatMapFunction[String, JSONObject, String] {\n" +
        "\n" +
        "        // downsample parameters\n" +
        "        var passProbability: Double = 1.0\n" +
        "\n" +
        "        // filter by attribute\n" +
        "        var neLat: Double = 0.0\n" +
        "        var neLng: Double = 0.0\n" +
        "        var swLat: Double = 0.0\n" +
        "        var swLng: Double = 0.0\n" +
        "\n" +
        "        override def flatMap2(elem: JSONObject, collector: Collector[String]): Unit = {\n" +
        "\n" +
        "\n" +
        "          val control = elem.get(\"control\").asInstanceOf[JSONObject]\n" +
        "\n" +
        "          val bounds = control.get(\"bounds\").asInstanceOf[JSONObject]\n" +
        "\n" +
        "          val northEast = bounds.get(\"_northEast\").asInstanceOf[JSONObject]\n" +
        "          val southWest = bounds.get(\"_southWest\").asInstanceOf[JSONObject]\n" +
        "\n" +
        "          neLat = northEast.get(\"lat\").asInstanceOf[Double]\n" +
        "          neLng = northEast.get(\"lng\").asInstanceOf[Double]\n" +
        "\n" +
        "          swLat = southWest.get(\"lat\").asInstanceOf[Double]\n" +
        "          swLng = southWest.get(\"lng\").asInstanceOf[Double]\n" +
        "\n" +
        "          var items = elem.get(\"num_items\").asInstanceOf[Integer]\n" +
        "\n" +
        "          println(\"got control message!\")\n" +
        "          println(\"nelat:\"+ neLat + \"neLng:\" + neLng + \"swLat:\" + swLat + \"swLng:\" + swLng)\n" +
        "        }\n" +
        "\n" +
        "        override def flatMap1(in: String, collector: Collector[String]): Unit = {\n" +
        "\n" +
        "          val parser: JSONParser = new JSONParser\n" +
        "\n" +
        "          val elem = parser.parse(in).asInstanceOf[JSONObject]\n" +
        "          var resString = \"\"\n" +
        "\n" +
        "          var entities = elem.get(\"entities\").asInstanceOf[JSONObject]\n" +
        "          var hashtags = entities.get(\"hashtags\").asInstanceOf[JSONArray]\n" +
        "\n" +
        "          if (hashtags.size() > 0) {\n" +
        "            val hashtag: JSONObject = hashtags.get(0).asInstanceOf[JSONObject]\n" +
        "            val hashtagtext: String = hashtag.get(\"text\").asInstanceOf[String]\n" +
        "            resString = hashtagtext\n" +
        "          } else {\n" +
        "            resString = \"hello\"\n" +
        "          }\n" +
        "\n" +
        "          //fill location statistics\n" +
        "          val coordinates = elem.get(\"coordinates\").asInstanceOf[JSONObject]\n" +
        "          if (coordinates != null) {\n" +
        "            val coordinates1: JSONArray = coordinates.get(\"coordinates\").asInstanceOf[JSONArray]\n" +
        "\n" +
        "            // TODO: sometin's messed up yo!\n" +
        "            val lng: Double = coordinates1.get(0).asInstanceOf[Double]\n" +
        "            val lat: Double = coordinates1.get(1).asInstanceOf[Double]\n" +
        "\n" +
        "            println(\"collecting:\" + lat + \" and \" + lng)\n" +
        "            println(neLat + \",\" + neLng + \"; \" + swLat + \",\" + swLng)\n" +
        "            // if its inside of boundingbox:\n" +
        "            if (lat < neLat && lat > swLat && lng < neLng && lng > swLng) {\n" +
        "              collector.collect(resString + \"\\t\" + lat + \"\\t\" + lng + \"\\n\")\n" +
        "            }\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "    \n" +
        "    \n" +
        "    \n" +
        "\n";

        String flinkIterator = "var it = collect(tablerows).asInstanceOf[DataStreamIterator[String]]\n" +
        "var tableString: String = \"%table\\n\\tname\\tothervalue\\tlat\\tlng\"\n" +
        "for(i <- 0 to 100){\n" +
        "    if(it.hasImmidiateNext(1)){\n" +
        "        var elString = it.next\n" +
        "        print(\"hello\" + i + \"\\t\" + elString)\n" +
        "    }\n" +
        "}";
    InterpreterResult result = flink.interpret(flinkProgram, context);

    System.setOut(outOld);
    System.out.println("code:\n" + result.code());
    System.out.println("msg:\n" + result.message());

//    assertEquals(Code.SUCCESS, result.code());

    PrintStream outOld2 = System.out;
//    flink.interpret("var tableString: String = \"%table\\n\\tname\\tothervalue\\tlat\\tlng\"",context);
//    flink.interpret("for(i <- 0 to 100){",context);
//    flink.interpret("if(it.hasImmidiateNextFromStream){",context);
//    flink.interpret("var elString = it.next",context);
//    flink.interpret("print(\"hello\" + i + \"\t\" + elString)",context);
//    flink.interpret("}",context);


    String inputCode = "var tableString: String = \"%table\\n\\tname\\tothervalue\\tlat\\tlng\"\n" +
            "    for(i <- 0 to 100){\n" +
            "    if(it.hasImmidiateNext){\n" +
            "        var elString = it.next\n" +
            "        print(\"hello\" + i + \"\\t\" + elString)\n" +
            "      }\n" +
            "    }";

    InterpreterResult result2 = flink.interpret(flinkIterator, context);

    System.setOut(outOld);
    System.out.println("INTERPRETER RESULT ITERATOR ITERATION:::://");
    System.out.println("code:\n" + result2.code());
    System.out.println("msg:\n" + result2.message());



//    assertEquals(Code.SUCCESS, result.code());
  }

  @Test
  public void othertest() {
    PrintStream outOld = System.out;
//    flink.interpret("var tableString: String = \"%table\\n\\tname\\tothervalue\\tlat\\tlng\"",context);
//    flink.interpret("for(i <- 0 to 100){",context);
//    flink.interpret("if(it.hasImmidiateNextFromStream){",context);
//    flink.interpret("var elString = it.next",context);
//    flink.interpret("print(\"hello\" + i + \"\t\" + elString)",context);
//    flink.interpret("}",context);


    String inputCode = "var tableString: String = \"%table\\n\\tname\\tothervalue\\tlat\\tlng\"\n" +
            "    for(i <- 0 to 100){\n" +
            "    if(it.hasImmidiateNextFromStream){\n" +
            "        var elString = it.next\n" +
            "        print(\"hello\" + i + \"\\t\" + elString)\n" +
            "      }\n" +
            "    }";
    InterpreterResult result = flink.interpret(inputCode,context);

    System.setOut(outOld);
    System.out.println("INTERPRETER RESULT ITERATOR ITERATION:::://");
    System.out.println("code:\n" + result.code());
    System.out.println("msg:\n" + result.message());
  }

  @Test
  public void testWordCount() {
    /*
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print

    env.execute("Scala Socket Stream WordCount")
     */
    //flink.interpret("val text = env.socketTextStream(\"localhost\", 9999)", context);
    PrintStream outOld = System.out;

    flink.interpret("val text = env.fromElements(\"To be or not to be\")",context);
    flink.interpret("val counts = text.flatMap { _.toLowerCase.split(\"\\\\W+\") filter { _.nonEmpty } }.map { (_, 1) }.keyBy(0).sum(1)", context);
//    flink.interpret("counts.print",context);
    flink.interpret("text.print",context);
    InterpreterResult result = flink.interpret("env.execute(\"Scala Socket Stream WordCount\")", context);
    System.setOut(outOld);
    System.out.println("INTERPRETER RESULT ITERATOR ITERATION:::://");
    System.out.println(result.message());

    assertEquals(Code.SUCCESS, result.code());
  }

  @Test
  public void testWordCountCollect() {
    PrintStream outOld = System.out;
    String message = flink.interpret("val text = env.fromElements(\"To be or not to be\")", context).message();
    System.setOut(outOld);
    System.out.println(message);

    message = flink.interpret("val counts = text.flatMap { _.toLowerCase.split(\"\\\\W+\") filter { _.nonEmpty } }.map { (_, 1) }.keyBy(0).sum(1)", context).message();
    System.setOut(outOld);
    System.out.println(message);

    message = flink.interpret("counts.print", context).message();
    System.setOut(outOld);
    System.out.println(message);


    //message = flink.interpret("import org.apache.flink.contrib.streaming.DataStreamUtils._", context).message();
    message = flink.interpret("import org.apache.flink.contrib.streaming.scala.DataStreamUtils._", context).message();
    System.setOut(outOld);
    System.out.println(message);

    message = flink.interpret("var collected = collect[(String,Int)](counts)", context).message();
    System.setOut(outOld);
    System.out.println(message);

    InterpreterResult result = flink.interpret("while(collected.hasNext()){var el = collected.next();println(\"next element:\" + el) }",context);
    System.setOut(outOld);
    System.out.println(message);

    System.out.println("res:" + result.message());
    //InterpreterResult result = flink.interpret("env.execute(\"Scala Socket Stream WordCount\")", context);
    assertEquals(Code.SUCCESS, result.code());
  }
}
