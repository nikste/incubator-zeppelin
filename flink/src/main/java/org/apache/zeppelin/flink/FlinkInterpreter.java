/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zeppelin.flink;


import org.apache.commons.io.IOUtils;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import scala.collection.mutable.ArrayBuffer;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Nikolaas Steenbergen on 8-5-15.
 */


/**
 * Interface for interpreters.
 * If you want to implement new Zeppelin interpreter, extend this class
 * <p/>
 * Please see,
 * http://zeppelin.incubator.apache.org/docs/development/writingzeppelininterpreter.html
 * <p/>
 * open(), close(), interpreter() is three the most important method you need to implement.
 * cancel(), getProgress(), completion() is good to have
 * getFormType(), getScheduler() determine Zeppelin's behavior
 */
public class FlinkInterpreter extends Interpreter {

  public FlinkInterpreter(Properties property)
  {
    super(property);
  }
  FlinkILoop repl;


  PipedWriter pw;
  PipedReader pr;
  /**
   * Opens interpreter. You may want to place your initialize routine here.
   * open() is called only once
   */
  @Override
  public void open() {
    // initialize flink-scala-shell (like in tests?)
    // start server:
    System.out.println("Starting Flink Shell:");

    LocalFlinkMiniCluster cluster = new LocalFlinkMiniCluster(new Configuration(), false);

    String host = "localhost";
    int port = cluster.getJobManagerRPCPort();

    // either port or userhost not specified by user, create new minicluster
    /*
    val (host,port) = if (userHost == "none" || userPort == -1 )
    {
      println("Creating new local server")
      cluster = new LocalFlinkMiniCluster(new Configuration, false)
      ("localhost",cluster.getJobManagerRPCPort)
    } else {
      println(s"Connecting to remote server (host: $userHost, port: $userPort).")
      (userHost, userPort)
    }*/




    // custom shell
    //BufferedReader reader = new BufferedReader();
    //BufferedWriter writer = new BufferedWriter();


    //String input = "";
    //StringReader sr = new StringReader(input + "\n");


    pw = new PipedWriter();

    BufferedReader in = new BufferedReader(pr);
    //InputStream is = new InputStream();
    //BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
    //StringWriter buffer = new StringWriter();
    //PrintWriter in = new PrintWriter(buffer);

    StringWriter out = new StringWriter();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // this we do need!
    System.setOut(new PrintStream(baos));

    ClassLoader cl = getClass().getClassLoader();
    String pathSeparator = File.pathSeparator;
    String classpath = "";
    URLClassLoader urlLoader = (URLClassLoader) cl;
    for (URL url : urlLoader.getURLs()) {
      if (url.getProtocol() == "file") {
        classpath += pathSeparator + url.getFile();
      }
    }

    repl = new FlinkILoop(host, port, in, new PrintWriter(out)); //new MyILoop();

    String[] classPathArg = {"-classpath", classpath};

    // starting repl
    repl.process(classPathArg);

  }


  /**
   * Closes interpreter. You may want to free your resources up here.
   * close() is called only once
   */
  @Override
  public void close() {
    repl.closeInterpreter();
  }

  /**
   * Run code and return result, in synchronous way.
   *
   * @param st    statements to run
   * @param context
   * @return
   */
  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    // call flink-scala-shell like in tests (give string)
    try {
      pw.write(st);
    } catch ( IOException e)
    {
      //TODO(future-me): show this to the user somehow.
      //logger.info("Interpreter exception", e);
    }

    //out.toString() + baos.toString();
    InterpreterResult ir  = new InterpreterResult(InterpreterResult.Code.SUCCESS);
    return (ir);
  }


  /**
   * Optionally implement the canceling routine to abort interpret() method
   *
   * @param context
   */
  @Override
  public void cancel(InterpreterContext context) {

  }

  /**
   * Dynamic form handling
   * see http://zeppelin.incubator.apache.org/docs/dynamicform.html
   *
   * @return FormType.SIMPLE enables simple pattern replacement (eg. Hello ${name=world}),
   * FormType.NATIVE handles form in API
   */
  @Override
  public FormType getFormType() {
    return null;
  }


  /**
   * get interpret() method running process in percentage.
   *
   * @param context
   * @return number between 0-100
   */
  @Override
  public int getProgress(InterpreterContext context) {
    // how would one do that?
    return 0;
  }


  /**
   * Get completion list based on cursor position.
   * By implementing this method, it enables auto-completion.
   *
   * @param buf  statements
   * @param cursor cursor position in statements
   * @return list of possible completion. Return empty list if there're nothing to return.
   */
  @Override
  public List<String> completion(String buf, int cursor) {
    // how can we do this, get the autocompletion out of FlinkILoop?!
    return null;
  }
}
