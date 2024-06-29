/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.udc.fic.ri;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.Date;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import java.io.FileReader;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.DirectoryStream;

import java.time.Duration;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.System.exit;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * For the folder given as argument, the class SimpleThreadPool1
 * prints the name of each subfolder using a
 * different thread.
 */


class ThreadPool {

    /**
     * This Runnable takes a folder and prints its path.
     */
    public static class WorkerThread implements Runnable {

        private final Path path;
        private final Path docsPath;

        private final boolean threadVerbose;

        private final IndexWriter writer;

        private final String[] onlyDomsList;

        private final boolean titleTermVectors;
        private final boolean bodyTermVectors;

        public WorkerThread(final Path path, Path docsPath, boolean threadVerbose, IndexWriter writer, String[] onlyDomsList, boolean titleTermVectors, boolean bodyTermVectors) {
            this.path = path;
            this.docsPath = docsPath;
            this.threadVerbose = threadVerbose;
            this.writer = writer;
            this.onlyDomsList = onlyDomsList;
            this.titleTermVectors = titleTermVectors;
            this.bodyTermVectors = bodyTermVectors;
        }

        private void indexDoc(IndexWriter writer, Path fileLoc, Path fileLocNoTags) throws IOException {
            try (InputStream stream = Files.newInputStream(fileLoc);
                 InputStream stream2 = Files.newInputStream(fileLocNoTags)) {
                Document doc = new Document();

                doc.add(new KeywordField("path", fileLoc.toString(), Field.Store.YES));

                doc.add(
                        new TextField(
                                "contents",
                                new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

                doc.add(new KeywordField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));

                doc.add(new KeywordField("thread", Thread.currentThread().getName(), Field.Store.YES));

                doc.add(new LongField("locKb", Files.size(fileLoc) / 1024, Field.Store.YES));

                doc.add(new StringField("creationTime", Files.getLastModifiedTime(fileLoc).toString(), Field.Store.YES));

                doc.add(new StringField("lastAccessTime", Files.getFileAttributeView(fileLoc, BasicFileAttributeView.class).readAttributes().lastAccessTime().toString(), Field.Store.YES));

                doc.add(new StringField("lastModifiedTime", Files.getFileAttributeView(fileLoc, BasicFileAttributeView.class).readAttributes().lastModifiedTime().toString(), Field.Store.YES));


                Date date = new Date(Files.getLastModifiedTime(fileLoc).toMillis());
                String stringDate = DateTools.dateToString(date, DateTools.Resolution.MILLISECOND);

                doc.add(new StringField("creationTimeLucene", stringDate, Field.Store.YES));

                Date date2 = new Date(Files.getFileAttributeView(fileLoc, BasicFileAttributeView.class).readAttributes().lastAccessTime().toMillis());
                String stringDate2 = DateTools.dateToString(date2, DateTools.Resolution.MILLISECOND);

                doc.add(new StringField("lastAccessTimeLucene", stringDate2, Field.Store.YES));

                Date date3 = new Date(Files.getFileAttributeView(fileLoc, BasicFileAttributeView.class).readAttributes().lastModifiedTime().toMillis());
                String stringDate3 = DateTools.dateToString(date3, DateTools.Resolution.MILLISECOND);

                doc.add(new StringField("lastModifiedTimeLucene", stringDate3, Field.Store.YES));


                doc.add(new LongField("notagsKb", Files.size(fileLocNoTags) / 1024, Field.Store.YES));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream2, StandardCharsets.UTF_8));

                if(titleTermVectors){
                    FieldType fieldType = new FieldType();
                    fieldType.setStored(true);
                    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                    fieldType.setStoreTermVectors(true);
                    fieldType.setStoreTermVectorPositions(true);
                    fieldType.setStoreTermVectorOffsets(true);
                    Field titleField = new Field("title", bufferedReader.readLine(), fieldType);
                    doc.add(titleField);
                }else {
                    doc.add(new TextField("title", bufferedReader.readLine(), Field.Store.YES));
                }

                StringBuilder bodyContent = new StringBuilder();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    bodyContent.append(line);
                    bodyContent.append("\n");
                }
                if(bodyTermVectors){
                    FieldType fieldType = new FieldType();
                    fieldType.setStored(true);
                    fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                    fieldType.setStoreTermVectors(true);
                    fieldType.setStoreTermVectorPositions(true);
                    fieldType.setStoreTermVectorOffsets(true);
                    Field bodyField = new Field("body", bodyContent.toString(), fieldType);
                    doc.add(bodyField);
                }else {
                    doc.add(new TextField("body", bodyContent.toString(), Field.Store.YES));
                }

                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                    // New index, so we just add the document (no old document can be there):
//                    System.out.println("adding " + fileLoc);
                    writer.addDocument(doc);
                } else {
                    // Existing index (an old copy of this document may have been indexed) so
                    // we use updateDocument instead to replace the old one matching the exact
                    // path, if present:
//                    System.out.println("updating " + fileLoc);
                    writer.updateDocument(new Term("path", fileLoc.toString()), doc);
                }
            } catch (IOException e) {
                System.out.println("Error indexing file " + fileLoc);
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {

                String name = Thread.currentThread().getName();

                if(!path.toString().endsWith(".url")) {
                    return;
                }

                if (threadVerbose) System.out.printf("Hilo " + name +" comienzo url " + this.path + "\n");

                BufferedReader reader = new BufferedReader(new FileReader(path.toFile()));

                String url;
                boolean matches = false;
                while ((url = reader.readLine()) != null) {
                    URI uri = URI.create(url);
                    String host = uri.getHost();
                    if(host == null) continue;
                    if(onlyDomsList != null) {
                        for (String dom : onlyDomsList) {
                            if (host.endsWith(dom)) {
                                matches = true;
                                break;
                            }
                        }
                    }
                    if(onlyDomsList != null && !matches) continue;
                    matches = false;
                    HttpClient httpClient = HttpClient.newHttpClient();
                    HttpRequest httpRequest;
                    try{
                        httpRequest = HttpRequest.newBuilder()
                                .uri(uri)
                                .timeout(Duration.of(10, SECONDS))
                                .GET()
                                .build();
                    }
                    catch (Exception e){
                        System.out.println("Error in request: " + uri);
                        continue;
                    }

                    HttpResponse<String> response = makeRequest(httpClient, httpRequest, 0, uri.toString());

                    if(response == null){
                        System.out.println("Error in request: " + uri);
                        continue;
                    }
                    String filename = uri.getHost() + uri.getPath().replace("/", "_");
                    if(filename.endsWith("_")) filename = filename.substring(0, filename.length() - 1);

                    Path path = Paths.get(docsPath.toString(),filename + ".loc");
                    Files.write(path, response.body().getBytes());


                    org.jsoup.nodes.Document doc = Jsoup.parse(response.body());
                    Element title = doc.select("title").first();
                    Element body = doc.select("body").first();

                    //Save them in a document, first line is title and the rest is body
                    Path filePath = Paths.get(docsPath.toString(), filename + ".loc.notags");
                    Files.write(filePath, (title.text() + "\n" + body.text()).getBytes());


                    indexDoc(writer, path, filePath);
                    if (threadVerbose) System.out.printf("Hilo "+ name + " fin url " + this.path + "\n");
                }
                reader.close();


            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * This is the work that the current thread will do when processed by the pool.
     * In this case, it will only print some information.
     */

    static HttpResponse<String> makeRequest(HttpClient httpClient, HttpRequest httpRequest, int iterations, String prevUrl) {
        try {
            if (iterations > 5) return null;
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

            int statusCode = response.statusCode();
            if (statusCode == 200) {
                return response;
            } else if (statusCode >= 300 && statusCode <= 400) {
                URI uri = URI.create(prevUrl + response.headers().firstValue("Location").get());
                makeRequest(httpClient, HttpRequest.newBuilder()
                        .uri(response.headers().firstValue("Location").map(URI::create).orElseThrow())
                        .timeout(Duration.of(10, SECONDS))
                        .GET()
                        .build(), iterations + 1, prevUrl);
            } else return null;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class WebIndexer implements AutoCloseable {

    /** Index all text files under a directory. */
    public static void main(String[] args) {
        String usage =
                "java es.udc.fic.ri.WebIndexer"
                        + " [-index INDEX_PATH] [-docs DOCS_PATH] [-create] [-numThreads NUM_THREADS] [-h] [-p] [-analyzer ANALYZER] [-titleTermVectors] [-bodyTermVectors]\n\n"
                        + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                        + "in INDEX_PATH that can be searched with SearchFiles\n";
        String indexPath = null;
        String docsPath = null;
        boolean create = false;
        int numThreads = Runtime.getRuntime().availableProcessors();
        boolean threadVerbose = false;
        boolean indexVerbose = false;
        boolean titleTermVectors = false;
        boolean bodyTermVectors = false;
        Analyzer analyzer = new StandardAnalyzer();
        Long time = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-create":
                    create = true;
                    break;
                case "-numThreads":
                    numThreads = Integer.parseInt(args[++i]);
                    break;
                case "-h":
                    threadVerbose = true;
                    break;
                case "-p":
                    indexVerbose = true;
                    break;
                case "-titleTermVectors":
                    titleTermVectors = true;
                    break;
                case "-bodyTermVectors":
                    bodyTermVectors = true;
                    break;
                case "-analyzer":
                    analyzer = selectAnalyzer(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (docsPath == null || indexPath == null) {
            System.err.println("Usage: " + usage);
            exit(1);
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println(
                    "Document directory '"
                            + docDir.toAbsolutePath()
                            + "' does not exist or is not readable, please check the path");
            exit(1);
        }

        try {
            String[] onlyDomsList = null;
            if (Files.exists(Path.of("src/main/resources/config.properties"))) {
                Properties properties = new Properties();
                BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(WebIndexer.class.getResourceAsStream("/config.properties"))));
                properties.load(reader);
                String onlyDoms = properties.getProperty("onlyDoms");
                onlyDomsList = onlyDoms.split("\\s+");
            }

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            if(indexVerbose){
                time = System.currentTimeMillis();
            }
            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }
            try(IndexWriter writer = new IndexWriter(dir, iwc)) {
                searchDirectory(null, numThreads, threadVerbose, docDir, writer, onlyDomsList, titleTermVectors, bodyTermVectors);
            }
            if(indexVerbose){
                System.out.println("Creado índice " + indexPath + " en " + (System.currentTimeMillis() - time) + " msecs");
            }
    } catch (Exception e){
            System.out.println("Error indexing files");
            e.printStackTrace();
        }
    }

    static Analyzer selectAnalyzer(String analyzer) {
        switch (analyzer) {
            case "standard":
                return new StandardAnalyzer();
            case "english":
                return new EnglishAnalyzer();
            case "spanish":
                return new SpanishAnalyzer();
            default:
                throw new IllegalArgumentException("unknown analyzer " + analyzer);
        }
    }

    static void searchDirectory(Path pathDir, int numThreads, boolean threadVerbose, Path destDir ,IndexWriter writer, String[] onlyDomsList, boolean titleTermVectors, boolean bodyTermVectors){

        Path docsPath = pathDir == null ? Paths.get("src/test/resources/urls") : pathDir;
        final int numCores = numThreads <= 0 ? Runtime.getRuntime().availableProcessors() : numThreads;
        final ExecutorService executor = Executors.newFixedThreadPool(numCores);

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docsPath)) {

            for (final Path path : directoryStream) {
                if (Files.isDirectory(path)) {
                    searchDirectory(path, numThreads, threadVerbose, destDir, writer, onlyDomsList, titleTermVectors, bodyTermVectors);
                }
                else{
                    final Runnable worker = new ThreadPool.WorkerThread(path, destDir, threadVerbose, writer, onlyDomsList, titleTermVectors, bodyTermVectors);
                    executor.execute(worker);
                }
            }

        } catch (final IOException e) {
            e.printStackTrace();
            exit(-1);
        }

        /*
         * Close the ThreadPool; no more jobs will be accepted, but all the previously
         * submitted jobs will be processed.
         */
        executor.shutdown();

        /* Wait up to 1 hour to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (final InterruptedException e) {
            e.printStackTrace();
            exit(-2);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
