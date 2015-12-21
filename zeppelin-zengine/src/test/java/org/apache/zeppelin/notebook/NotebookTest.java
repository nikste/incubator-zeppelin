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

package org.apache.zeppelin.notebook;

import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.flink.FlinkInterpreterStreaming;
import org.apache.zeppelin.interpreter.InterpreterFactory;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.mock.MockInterpreter1;
import org.apache.zeppelin.interpreter.mock.MockInterpreter2;
import org.apache.zeppelin.notebook.repo.NotebookRepo;
import org.apache.zeppelin.notebook.repo.VFSNotebookRepo;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.Job.Status;
import org.apache.zeppelin.scheduler.JobListener;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class NotebookTest implements JobListenerFactory{

  private File tmpDir;
  private ZeppelinConfiguration conf;
  private SchedulerFactory schedulerFactory;
  private File notebookDir;
  private Notebook notebook;
  private NotebookRepo notebookRepo;
  private InterpreterFactory factory;

  @Before
  public void setUp() throws Exception {
//    tmpDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis());
//    tmpDir.mkdirs();
//    new File(tmpDir, "conf").mkdirs();
//    notebookDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis()+"/notebook");
//    notebookDir.mkdirs();
//
//    System.setProperty(ConfVars.ZEPPELIN_HOME.getVarName(), tmpDir.getAbsolutePath());
//    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(), notebookDir.getAbsolutePath());
//    System.setProperty(ConfVars.ZEPPELIN_INTERPRETERS.getVarName(), "org.apache.zeppelin.interpreter.mock.MockInterpreter1,org.apache.zeppelin.interpreter.mock.MockInterpreter2");
//
//    conf = ZeppelinConfiguration.create();
//
//    this.schedulerFactory = new SchedulerFactory();
//
//    MockInterpreter1.register("mock1", "org.apache.zeppelin.interpreter.mock.MockInterpreter1");
//    MockInterpreter2.register("mock2", "org.apache.zeppelin.interpreter.mock.MockInterpreter2");
//
//    factory = new InterpreterFactory(conf, new InterpreterOption(false), null);
//
//    notebookRepo = new VFSNotebookRepo(conf);
//    notebook = new Notebook(conf, notebookRepo, schedulerFactory, factory, this);
    tmpDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis());
    tmpDir.mkdirs();
    new File(tmpDir, "conf").mkdirs();
    notebookDir = new File(System.getProperty("java.io.tmpdir")+"/ZeppelinLTest_"+System.currentTimeMillis()+"/notebook");
    notebookDir.mkdirs();

    System.setProperty(ConfVars.ZEPPELIN_HOME.getVarName(), tmpDir.getAbsolutePath());
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_DIR.getVarName(), notebookDir.getAbsolutePath());
    System.setProperty(ConfVars.ZEPPELIN_INTERPRETERS.getVarName(), ConfVars.ZEPPELIN_INTERPRETERS.getStringValue());
/// /    System.setProperty(ConfVars.ZEPPELIN_INTERPRETERS.getVarName(), "org.apache.zeppelin.flink.FlinkInterpreterStreaming");

    conf = ZeppelinConfiguration.create();

    this.schedulerFactory = new SchedulerFactory();

    FlinkInterpreterStreaming.register("flinkStreaming","org.apache.zeppelin.flink.FlinkInterpreterStreaming");

    MockInterpreter1.register("mock1", "org.apache.zeppelin.interpreter.mock.MockInterpreter1");
    MockInterpreter2.register("mock2", "org.apache.zeppelin.interpreter.mock.MockInterpreter2");

    factory = new InterpreterFactory(conf, new InterpreterOption(false), null);

    notebookRepo = new VFSNotebookRepo(conf);
    notebook = new Notebook(conf, notebookRepo, schedulerFactory, factory, this);
  }

  @After
  public void tearDown() throws Exception {
    delete(tmpDir);
  }

  @Test
  public void testSelectingReplImplementation() throws IOException {
    Note note = notebook.createNote();
    note.getNoteReplLoader().setInterpreters(factory.getDefaultInterpreterSettingList());

    // run with defatul repl
    Paragraph p1 = note.addParagraph();
    p1.setText("hello world");
    note.run(p1.getId());
    while(p1.isTerminated()==false || p1.getResult()==null) Thread.yield();
    assertEquals("repl1: hello world", p1.getResult().message());

    // run with specific repl
    Paragraph p2 = note.addParagraph();
    p2.setText("%mock2 hello world");
    note.run(p2.getId());
    while(p2.isTerminated()==false || p2.getResult()==null) Thread.yield();
    assertEquals("repl2: hello world", p2.getResult().message());
  }

  @Test
  public void testGetAllNotes() throws IOException {
    // get all notes after copy the {notebookId}/note.json into notebookDir
    File srcDir = new File("src/test/resources/2A94M5J1Z");
    File destDir = new File(notebookDir.getAbsolutePath() + "/2A94M5J1Z");

    try {
      FileUtils.copyDirectory(srcDir, destDir);
    } catch (IOException e) {
      e.printStackTrace();
    }

    Note copiedNote = notebookRepo.get("2A94M5J1Z");

    // when ZEPPELIN_NOTEBOOK_GET_FROM_REPO set to be false
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_RELOAD_FROM_STORAGE.getVarName(), "false");
    List<Note> notes = notebook.getAllNotes();
    assertEquals(notes.size(), 0);

    // when ZEPPELIN_NOTEBOOK_GET_FROM_REPO set to be true
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_RELOAD_FROM_STORAGE.getVarName(), "true");
    notes = notebook.getAllNotes();
    assertEquals(notes.size(), 1);
    assertEquals(notes.get(0).id(), copiedNote.id());
    assertEquals(notes.get(0).getName(), copiedNote.getName());
    assertEquals(notes.get(0).getParagraphs(), copiedNote.getParagraphs());

    // get all notes after remove the {notebookId}/note.json from notebookDir
    // when ZEPPELIN_NOTEBOOK_GET_FROM_REPO set to be false
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_RELOAD_FROM_STORAGE.getVarName(), "false");
    // delete the notebook
    FileUtils.deleteDirectory(destDir);
    notes = notebook.getAllNotes();
    assertEquals(notes.size(), 1);

    // when ZEPPELIN_NOTEBOOK_GET_FROM_REPO set to be true
    System.setProperty(ConfVars.ZEPPELIN_NOTEBOOK_RELOAD_FROM_STORAGE.getVarName(), "true");
    notes = notebook.getAllNotes();
    assertEquals(notes.size(), 0);
  }

  @Test
  public void testPersist() throws IOException, SchedulerException{
    Note note = notebook.createNote();

    // run with default repl
    Paragraph p1 = note.addParagraph();
    p1.setText("hello world");
    note.persist();

    Notebook notebook2 = new Notebook(conf, notebookRepo, schedulerFactory, new InterpreterFactory(conf, null), this);
    assertEquals(1, notebook2.getAllNotes().size());
  }

  @Test
  public void testClearParagraphOutput() throws IOException, SchedulerException{
    Note note = notebook.createNote();
    Paragraph p1 = note.addParagraph();
    p1.setText("hello world");
    note.run(p1.getId());

    while(p1.isTerminated()==false || p1.getResult()==null) Thread.yield();
    assertEquals("repl1: hello world", p1.getResult().message());

    // clear paragraph output/result
    note.clearParagraphOutput(p1.getId());
    assertNull(p1.getResult());
  }

  @Test
  public void testRunAll() throws IOException {
    Note note = notebook.createNote();
    note.getNoteReplLoader().setInterpreters(factory.getDefaultInterpreterSettingList());

    Paragraph p1 = note.addParagraph();
    p1.setText("p1");
    Paragraph p2 = note.addParagraph();
    p2.setText("p2");
    assertEquals(null, p2.getResult());
    note.runAll();

    while(p2.isTerminated()==false || p2.getResult()==null) Thread.yield();
    assertEquals("repl1: p2", p2.getResult().message());
  }

  @Test
  public void testAndIterateBitch() throws IOException, InterruptedException {

    String flinkProgram = "%flinkStreaming\n" +
            "import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction\n" +
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

    String flinkIterator = "%flinkStreaming\n" +
            "var it = collect(tablerows).asInstanceOf[DataStreamIterator[String]]\n" +
            "var tableString: String = \"%table\\n\\tname\\tothervalue\\tlat\\tlng\"\n" +
            "for(i <- 0 to 100){\n" +
            "    var hin = it.hasImmidiateNext(1)\n" +
            "    if(hin){\n" +
            "        var elString = it.next\n" +
            "        print(\"hello\" + i + \"\\t\" + elString)\n" +
            "    }\n" +
            "}";
    Note note = notebook.createNote();
//    List<Interpreter.RegisteredInterpreter> registeredInterpreterList = factory.getRegisteredInterpreterList();

//    note.getNoteReplLoader().setInterpreters(registeredInterpreterList);//factory.getRegisteredInterpreterList());
//    Paragraph p1 = note.addParagraph();
//    p1.setText(flinkProgram);
//    Paragraph p2 = note.addParagraph();
//    p2.setText(flinkIterator);
//    assertEquals(null, p2.getResult());
//    note.runAll();
    Paragraph p1 = note.addParagraph();
    p1.setText(flinkProgram);
    Paragraph p2 = note.addParagraph();
    p2.setText(flinkIterator);
//    p1.run();
//    p2.run();

    note.runAll();

//    Paragraph paragraph = note.addParagraph();
//    paragraph.setText("%flinkStreaming\n" +
//                      "var els = env.fromElements(\"a\",\"b\")\n" +
//                      "els.print\n" +
//                      "env.execute()");
//    paragraph.run();
//
//    boolean running = paragraph.isRunning();
//    boolean terminated = paragraph.isTerminated();
//    InterpreterResult result = paragraph.getResult();

    boolean running22 = p2.isRunning();
    boolean terminated22 = p2.isTerminated();
    InterpreterResult result22 = p2.getResult();


    boolean running32 = p1.isRunning();
    boolean terminated32 = p1.isTerminated();
    InterpreterResult result32 = p1.getResult();
    while(p2.isRunning() == true || p2.getResult() == null) {

      boolean running2 = p2.isRunning();
      boolean terminated2 = p2.isTerminated();
      InterpreterResult result2 = p2.getResult();


      boolean running3 = p1.isRunning();
      boolean terminated3 = p1.isTerminated();
      InterpreterResult result3 = p1.getResult();

      Thread.yield();
    }

    System.out.println(p2.getResult().message());
//    while(p2.isTerminated()==false || p2.getResult()==null) Thread.yield();
//    assertEquals("repl1: p2", p2.getResult().message());

  }

  @Test
  public void testSchedule() throws InterruptedException, IOException{
    // create a note and a paragraph
    Note note = notebook.createNote();
    note.getNoteReplLoader().setInterpreters(factory.getDefaultInterpreterSettingList());

    Paragraph p = note.addParagraph();
    p.setText("p1");
    Date dateFinished = p.getDateFinished();
    assertNull(dateFinished);

    // set cron scheduler, once a second
    Map<String, Object> config = note.getConfig();
    config.put("cron", "* * * * * ?");
    note.setConfig(config);
    notebook.refreshCron(note.id());
    Thread.sleep(1*1000);
    
    // remove cron scheduler.
    config.put("cron", null);
    note.setConfig(config);
    notebook.refreshCron(note.id());
    Thread.sleep(1000);
    dateFinished = p.getDateFinished();
    assertNotNull(dateFinished);
    Thread.sleep(1*1000);
    assertEquals(dateFinished, p.getDateFinished());
  }

  @Test
  public void testCloneNote() throws IOException, CloneNotSupportedException,
      InterruptedException {
    Note note = notebook.createNote();
    note.getNoteReplLoader().setInterpreters(factory.getDefaultInterpreterSettingList());

    final Paragraph p = note.addParagraph();
    p.setText("hello world");
    note.runAll();
    while(p.isTerminated()==false || p.getResult()==null) Thread.yield();

    p.setStatus(Status.RUNNING);
    Note cloneNote = notebook.cloneNote(note.getId(), "clone note");
    Paragraph cp = cloneNote.paragraphs.get(0);
    assertEquals(cp.getStatus(), Status.READY);
    assertNotEquals(cp.getId(), p.getId());
    assertEquals(cp.text, p.text);
    assertEquals(cp.getResult().message(), p.getResult().message());
  }

  @Test
  public void testAngularObjectRemovalOnNotebookRemove() throws InterruptedException,
      IOException {
    // create a note and a paragraph
    Note note = notebook.createNote();
    note.getNoteReplLoader().setInterpreters(factory.getDefaultInterpreterSettingList());

    AngularObjectRegistry registry = note.getNoteReplLoader()
        .getInterpreterSettings().get(0).getInterpreterGroup()
        .getAngularObjectRegistry();

    // add local scope object
    registry.add("o1", "object1", note.id());
    // add global scope object
    registry.add("o2", "object2", null);

    // remove notebook
    notebook.removeNote(note.id());

    // local object should be removed
    assertNull(registry.get("o1", note.id()));
    // global object sould be remained
    assertNotNull(registry.get("o2", null));
  }

  @Test
  public void testAngularObjectRemovalOnInterpreterRestart() throws InterruptedException,
      IOException {
    // create a note and a paragraph
    Note note = notebook.createNote();
    note.getNoteReplLoader().setInterpreters(factory.getDefaultInterpreterSettingList());

    AngularObjectRegistry registry = note.getNoteReplLoader()
        .getInterpreterSettings().get(0).getInterpreterGroup()
        .getAngularObjectRegistry();

    // add local scope object
    registry.add("o1", "object1", note.id());
    // add global scope object
    registry.add("o2", "object2", null);

    // restart interpreter
    factory.restart(note.getNoteReplLoader().getInterpreterSettings().get(0).id());
    registry = note.getNoteReplLoader()
    .getInterpreterSettings().get(0).getInterpreterGroup()
    .getAngularObjectRegistry();

    // local and global scope object should be removed
    assertNull(registry.get("o1", note.id()));
    assertNull(registry.get("o2", null));
    notebook.removeNote(note.id());
  }

  @Test
  public void testAbortParagraphStatusOnInterpreterRestart() throws InterruptedException,
      IOException {
    Note note = notebook.createNote();
    note.getNoteReplLoader().setInterpreters(factory.getDefaultInterpreterSettingList());

    Paragraph p1 = note.addParagraph();
    p1.setText("p1");
    Paragraph p2 = note.addParagraph();
    p2.setText("p2");
    Paragraph p3 = note.addParagraph();
    p3.setText("p3");
    Paragraph p4 = note.addParagraph();
    p4.setText("p4");

    /* all jobs are ready to run */
    assertEquals(Job.Status.READY, p1.getStatus());
    assertEquals(Job.Status.READY, p2.getStatus());
    assertEquals(Job.Status.READY, p3.getStatus());
    assertEquals(Job.Status.READY, p4.getStatus());

	/* run all */
    note.runAll();

    /* all are pending in the beginning (first one possibly started)*/
    assertTrue(p1.getStatus() == Job.Status.PENDING || p1.getStatus() == Job.Status.RUNNING);
    assertEquals(Job.Status.PENDING, p2.getStatus());
    assertEquals(Job.Status.PENDING, p3.getStatus());
    assertEquals(Job.Status.PENDING, p4.getStatus());

    /* wait till first job is terminated and second starts running */
    while(p1.isTerminated() == false || (p2.getStatus() == Job.Status.PENDING)) Thread.yield();

    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(Job.Status.RUNNING, p2.getStatus());
    assertEquals(Job.Status.PENDING, p3.getStatus());
    assertEquals(Job.Status.PENDING, p4.getStatus());

    /* restart interpreter */
    factory.restart(note.getNoteReplLoader().getInterpreterSettings().get(0).id());

    /* pending and running jobs have been aborted */
    assertEquals(Job.Status.FINISHED, p1.getStatus());
    assertEquals(Job.Status.ABORT, p2.getStatus());
    assertEquals(Job.Status.ABORT, p3.getStatus());
    assertEquals(Job.Status.ABORT, p4.getStatus());
  }

  private void delete(File file){
    if(file.isFile()) file.delete();
    else if(file.isDirectory()){
      File [] files = file.listFiles();
      if(files!=null && files.length>0){
        for(File f : files){
          delete(f);
        }
      }
      file.delete();
    }
  }
  
  @Override
  public JobListener getParagraphJobListener(Note note) {
    return new JobListener(){

      @Override
      public void onProgressUpdate(Job job, int progress) {
      }

      @Override
      public void beforeStatusChange(Job job, Status before, Status after) {
      }

      @Override
      public void afterStatusChange(Job job, Status before, Status after) {
      }
    };
  }
}
