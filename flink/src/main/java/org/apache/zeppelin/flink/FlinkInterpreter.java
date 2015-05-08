/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zeppelin.flink;


import org.apache.flink.api.scala.FlinkILoop;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import scala.collection.mutable.ArrayBuffer;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;

/**
 * Created by Nikolaas Steenbergen on 8-5-15.
 */



/**
 * Interface for interpreters.
 * If you want to implement new Zeppelin interpreter, extend this class
 *
 * Please see,
 * http://zeppelin.incubator.apache.org/docs/development/writingzeppelininterpreter.html
 *
 * open(), close(), interpreter() is three the most important method you need to implement.
 * cancel(), getProgress(), completion() is good to have
 * getFormType(), getScheduler() determine Zeppelin's behavior
 *
 */
public class FlinkInterpreter extends Interpreter {


    public FlinkInterpreter(Properties property) {
        super(property);
    }



    FlinkILoop repl;
    /**
     * Opens interpreter. You may want to place your initialize routine here.
     * open() is called only once
     */
    @Override
    public void open() {
        // initialize flink-scala-shell (like in tests?)

        String input = "";
        BufferedReader in = new BufferedReader(new StringReader(input + "\n"));
        StringWriter out = new StringWriter();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // this we do need!
        System.setOut(new PrintStream(baos));

        // new local cluster
        String host = "localhost";
        // do we need this, not MultipleTestBase though
        Integer port = MultipleProgramsTestBase.cluster.getJobManagerRPCPort();

        ClassLoader cl = getClass().getClassLoader();
        ArrayBuffer<String> paths = new ArrayBuffer<String>();
        //if (cl.isInstanceOf[URLClassLoader]) {
            URLClassLoader urlLoader = (URLClassLoader) cl;
            for (URL url :urlLoader.getURLs()) {
                if (url.getProtocol() == "file") {
                    paths += url.getFile();
                    //paths += url.getFile();
                }
            }
        }
        val classpath = paths.mkString(File.pathSeparator)

        val repl = new FlinkILoop(host, port, in, new PrintWriter(out)) //new MyILoop();

        repl.process(Array("-classpath",classpath))

        repl.closeInterpreter

        out.toString + baos.toString()
    }


    /**
     * Closes interpreter. You may want to free your resources up here.
     * close() is called only once
     */
    @Override
    public void close() {

    }

    /**
     * Run code and return result, in synchronous way.
     *
     * @param st statements to run
     * @param context
     * @return
     */
    @Override
    public InterpreterResult interpret(String st, InterpreterContext context) {
        // call flink-scala-shell like in tests (give string)

        return null;
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
     *         FormType.NATIVE handles form in API
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
     * @param buf statements
     * @param cursor cursor position in statements
     * @return list of possible completion. Return empty list if there're nothing to return.
     */
    @Override
    public List<String> completion(String buf, int cursor) {
        // how can we do this, get the autocompletion out of FlinkILoop?!
        return null;
    }
}
