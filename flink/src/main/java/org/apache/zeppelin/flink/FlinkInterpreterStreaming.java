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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ScalaShellRemoteEnvironment;
import org.apache.flink.api.java.ScalaShellRemoteStreamEnvironment;
import org.apache.flink.api.scala.FlinkILoop;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Console;
import scala.None;
import scala.Option;
import scala.Some;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;
import scala.runtime.AbstractFunction0;
import scala.tools.nsc.Settings;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.interpreter.Results;
import scala.tools.nsc.settings.MutableSettings.BooleanSetting;
import scala.tools.nsc.settings.MutableSettings.PathSetting;

/**
 * Interpreter for Apache Flink (http://flink.apache.org)
 */
public class FlinkInterpreterStreaming extends Interpreter {
  Logger logger = LoggerFactory.getLogger(FlinkInterpreterStreaming.class);
  private ByteArrayOutputStream out;
  private Configuration flinkConf;
  private LocalFlinkMiniCluster localFlinkCluster;
  private FlinkILoop flinkIloop;
  private Map<String, Object> binder;
  private IMain imain;
  private ActorSystem actorSystem;

  public FlinkInterpreterStreaming(Properties property) {
    super(property);
  }

  static {
    Interpreter.register(
            "flinkStreaming",
            "flinkStreaming",
            FlinkInterpreterStreaming.class.getName(),
            new InterpreterPropertyBuilder()
                    .add("host", "local",
                            "host name of running JobManager. 'local' runs flink in local mode")
                    .add("jobmanager.rpc.port", "6124", "port of running JobManager")
                    .build()
    );
  }

  @Override
  public void open() {
    out = new ByteArrayOutputStream();
    flinkConf = new Configuration();
    Properties intpProperty = getProperty();
    for (Object k : intpProperty.keySet()) {
      String key = (String) k;
      String val = toString(intpProperty.get(key));
      flinkConf.setString(key, val);
    }

    if (localMode()) {
      try {
        startFlinkMiniCluster();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (TimeoutException e) {
        e.printStackTrace();
      }
    }

    int port  = getPort();
    String host = getHost();
    flinkIloop = new FlinkILoop(getHost(),
            getPort(),
            StreamingMode.STREAMING,
            (BufferedReader) null,
            new PrintWriter(out));

    flinkIloop.settings_$eq(createSettings());
    flinkIloop.createInterpreter();

    imain = flinkIloop.intp();

    StreamExecutionEnvironment env = (StreamExecutionEnvironment) flinkIloop.scalaEnv();

    env.getConfig().disableSysoutLogging();

    // prepare bindings
    imain.interpret("@transient var _binder = new java.util.HashMap[String, Object]()");
    binder = (Map<String, Object>) getValue("_binder");

    // import libraries
    imain.interpret("import scala.tools.nsc.io._");
    imain.interpret("import Properties.userHome");
    imain.interpret("import scala.compat.Platform.EOL");

    imain.interpret("import org.apache.flink.api.scala._");
    imain.interpret("import org.apache.flink.api.common.functions._");
    imain.bindValue("env", env);
  }

  private boolean localMode() {
    String host = getProperty("host");
    return host == null || host.trim().length() == 0 || host.trim().equals("local");
  }

  private String getHost() {
    if (localMode()) {
      return "localhost";
    } else {
      return getProperty("host");
    }
  }

  private int getPort() {
    if (localMode()) {
      return localFlinkCluster.getLeaderRPCPort();
    } else {
      return Integer.parseInt(getProperty("port"));
    }
  }

  private Settings createSettings() {
    URL[] urls = getClassloaderUrls();
    Settings settings = new Settings();

    // set classpath
    PathSetting pathSettings = settings.classpath();
    String classpath = "";
    List<File> paths = currentClassPath();
    for (File f : paths) {
      if (classpath.length() > 0) {
        classpath += File.pathSeparator;
      }
      classpath += f.getAbsolutePath();
    }

    if (urls != null) {
      for (URL u : urls) {
        if (classpath.length() > 0) {
          classpath += File.pathSeparator;
        }
        classpath += u.getFile();
      }
    }

    pathSettings.v_$eq(classpath);
    settings.scala$tools$nsc$settings$ScalaSettings$_setter_$classpath_$eq(pathSettings);
    settings.explicitParentLoader_$eq(new Some<ClassLoader>(Thread.currentThread()
            .getContextClassLoader()));
    BooleanSetting b = (BooleanSetting) settings.usejavacp();
    b.v_$eq(true);
    settings.scala$tools$nsc$settings$StandardScalaSettings$_setter_$usejavacp_$eq(b);

    return settings;
  }


  private List<File> currentClassPath() {
    List<File> paths = classPath(Thread.currentThread().getContextClassLoader());
    String[] cps = System.getProperty("java.class.path").split(File.pathSeparator);
    if (cps != null) {
      for (String cp : cps) {
        paths.add(new File(cp));
      }
    }
    return paths;
  }

  private List<File> classPath(ClassLoader cl) {
    List<File> paths = new LinkedList<File>();
    if (cl == null) {
      return paths;
    }

    if (cl instanceof URLClassLoader) {
      URLClassLoader ucl = (URLClassLoader) cl;
      URL[] urls = ucl.getURLs();
      if (urls != null) {
        for (URL url : urls) {
          paths.add(new File(url.getFile()));
        }
      }
    }
    return paths;
  }

  public Object getValue(String name) {
    IMain imain = flinkIloop.intp();
    Object ret = imain.valueOfTerm(name);
    if (ret instanceof None) {
      return null;
    } else if (ret instanceof Some) {
      return ((Some) ret).get();
    } else {
      return ret;
    }
  }

  @Override
  public void close() {
    flinkIloop.closeInterpreter();

    if (localMode()) {
      stopFlinkMiniCluster();
    }
  }

  @Override
  public InterpreterResult interpret(String line, InterpreterContext context) {
    if (line == null || line.trim().length() == 0) {
      return new InterpreterResult(Code.SUCCESS);
    }

    InterpreterResult result = interpret(line.split("\n"), context);
    return result;
  }

  public InterpreterResult interpret(String[] lines, InterpreterContext context) {
    final IMain imain = flinkIloop.intp();

    String[] linesToRun = new String[lines.length + 1];
    for (int i = 0; i < lines.length; i++) {
      linesToRun[i] = lines[i];
    }
    linesToRun[lines.length] = "print(\"\")";

    System.setOut(new PrintStream(out));
    out.reset();
    Code r = null;

    String incomplete = "";
    for (int l = 0; l < linesToRun.length; l++) {
      final String s = linesToRun[l];
      // check if next line starts with "." (but not ".." or "./") it is treated as an invocation
      if (l + 1 < linesToRun.length) {
        String nextLine = linesToRun[l + 1].trim();
        if (nextLine.startsWith(".") && !nextLine.startsWith("..") && !nextLine.startsWith("./")) {
          incomplete += s + "\n";
          continue;
        }
      }

      final String currentCommand = incomplete;

      scala.tools.nsc.interpreter.Results.Result res = null;
      try {
        res = Console.withOut(
            System.out,
            new AbstractFunction0<Results.Result>() {
            @Override
            public Results.Result apply() {
              return imain.interpret(currentCommand + s);
            }
          });
      } catch (Exception e) {
        logger.info("Interpreter exception", e);
        return new InterpreterResult(Code.ERROR, InterpreterUtils.getMostRelevantMessage(e));
      }

      r = getResultCode(res);

      if (r == Code.ERROR) {
        return new InterpreterResult(r, out.toString());
      } else if (r == Code.INCOMPLETE) {
        incomplete += s + "\n";
      } else {
        incomplete = "";
      }
    }

    if (r == Code.INCOMPLETE) {
      return new InterpreterResult(r, "Incomplete expression");
    } else {
      return new InterpreterResult(r, out.toString());
    }
  }

  private Code getResultCode(scala.tools.nsc.interpreter.Results.Result r) {
    if (r instanceof scala.tools.nsc.interpreter.Results.Success$) {
      return Code.SUCCESS;
    } else if (r instanceof scala.tools.nsc.interpreter.Results.Incomplete$) {
      return Code.INCOMPLETE;
    } else {
      return Code.ERROR;
    }
  }


  /*
  ActorGateway getJobManager() throws Exception {
    //TODO(nikste): Get ActorRef from YarnCluster if we are in YARN mode.

    InetSocketAddress address = new InetSocketAddress(flinkIloop.host(), flinkIloop.port());


    val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
            ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
    // start an actor system if needed
    if (this.actorSystem == null) {
      try {
        scala.Tuple2<String, Object> systemEndpoint = new scala.Tuple2<String, Object>("", 0);
        //GlobalConfiguration.getConfiguration();
        this.actorSystem = AkkaUtils.createActorSystem(
                 flinkConf,
                new Some<scala.Tuple2<String, Object>>(systemEndpoint));
      }
      catch (Exception e) {
        throw new IOException("Could not start actor system to communicate with JobManager", e);
      }
    }

    LeaderRetrievalService lrs1 =
            LeaderRetrievalUtils.createLeaderRetrievalService(flinkConf);
                    //GlobalConfiguration.getConfiguration());

    FiniteDuration lookupTimeout = new FiniteDuration(1000, TimeUnit.MILLISECONDS);
    return LeaderRetrievalUtils.retrieveLeaderGateway(lrs1, this.actorSystem, lookupTimeout);
  }
*/

  @Override
  public void cancel(InterpreterContext context) {
/*
    System.out.println("CANCELLING JOB!!!");
    StreamExecutionEnvironment env =
            (StreamExecutionEnvironment) flinkIloop.scalaEnv();


    try {
      ActorGateway jobManager = getJobManager();
      Future<Object> ask = jobManager.ask(
              JobManagerMessages.getRequestRunningJobsStatus(),
              new FiniteDuration(1000, TimeUnit.MILLISECONDS));

      Object result = Await.result(
              ask,
              new FiniteDuration(2000,
                      TimeUnit.MILLISECONDS));
      List<JobStatusMessage> jobs =
              ((JobManagerMessages.RunningJobsStatus) result)
                      .getStatusMessages();
      for (JobStatusMessage job : jobs) {
        Future response = jobManager.ask(
                new JobManagerMessages.CancelJob(job.getJobId()),
                new FiniteDuration(1000, TimeUnit.MILLISECONDS));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }*/
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    return new LinkedList<String>();
  }

  private void startFlinkMiniCluster() throws InterruptedException, TimeoutException {
    localFlinkCluster = new LocalFlinkMiniCluster(flinkConf, false, StreamingMode.STREAMING);
    localFlinkCluster.start();
    localFlinkCluster.waitForTaskManagersToBeRegistered();
  }

  private void stopFlinkMiniCluster() {
    if (localFlinkCluster != null) {
      localFlinkCluster.stop();
      localFlinkCluster = null;
    }
  }

  static final String toString(Object o) {
    return (o instanceof String) ? (String) o : "";
  }
}
