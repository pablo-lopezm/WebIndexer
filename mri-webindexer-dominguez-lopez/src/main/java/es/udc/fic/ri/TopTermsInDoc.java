package es.udc.fic.ri;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import static org.apache.lucene.queries.function.IndexReaderFunctions.maxDoc;

public class TopTermsInDoc {

    public static void main(String[] args) {
        String usage =
                "java es.udc.fic.ri.TopTermsInDoc"
                        + " [-index INDEX_PATH] [-field FIELD] [-docID DOC_ID] [-outfile OUTFILE_PATH] [-top TOP]\n\n"
                        + "This finds the top TOP terms for a field FIELD in document DOC_ID or url URL in index INDEX_PATH "
                        + "and stores the results in OUTFILE_PATH\n";
        String index = null;
        String field = null;
        int docID = -1;
        int topN = 10;
        String outfile = null;
        String url = null;

        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    index = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-docID":
                    docID = Integer.parseInt(args[++i]);
                    break;
                case "-top":
                    topN = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outfile = args[++i];
                    break;
                case "-url":
                    url = args[++i];
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.exit(1);
            }
        }

        if (index == null || field == null || outfile == null || (docID == -1 && url == null)) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        List<TermStats> termStats = new ArrayList<>();


        try {
            // Open index
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new MatchAllDocsQuery(), reader.maxDoc());

            if(docID == -1){
                url = url.replace("http://", "").replace("https://", "").replace("/", "_");
                if(url.endsWith("_")) url = url.substring(0, url.length() - 1);
                StoredFields storedFields = reader.storedFields();
                for (ScoreDoc hit : hits.scoreDocs) {
                    Document doc = storedFields.document(hit.doc);
                    Path p = Paths.get(doc.get("path"));
                    if (p.endsWith(url + ".loc")){
                        docID = hit.doc;
                        break;
                    }
                }
                if(docID == -1){
                    System.err.println("No document found for provided url");
                    System.exit(1);
                }
            }

            TermVectors termVectors = reader.termVectors();

            Terms terms = termVectors.get(docID,field);
            if (terms != null) {
                final TermsEnum termsEnum = terms.iterator();
                while (termsEnum.next() != null) {
                    int tf = (int) termsEnum.totalTermFreq();
                    int docFreq = termsEnum.docFreq();
                    double idf = Math.log10((double) reader.numDocs() / (double) docFreq);
                    double tfIdfLog10 = tf * idf;
                    termStats.add(new TermStats(termsEnum.term().utf8ToString(), tf, docFreq, tfIdfLog10));
                }
            }
            else {
                System.err.println("No term vector found for provided document / url in field " + field);
                System.exit(1);
            }
            termStats.sort(Comparator.comparingDouble(TermStats::getTfIdfLog10).reversed());

            try{
                    PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outfile)));
                    writer.println("Top " + topN + " terms for document " + docID + " in field " + field + ":\n");
                    System.out.println("Top " + topN + " terms for document " + docID + " in field " + field + ":\n");
                    termStats.stream().limit(topN).forEach(termStat -> {
                        writer.println(termStat);
                        System.out.println(termStat);
                    });
                    writer.close();
                }
            catch (IOException e){
                System.out.println("Cannot write to output file: exception " + e);
                e.printStackTrace();
            }

                reader.close();
        } catch (IOException e) {
            System.out.println("Cannot read indexes: exception " + e);
            e.printStackTrace();
        }
    }

    private static class TermStats {
        private final String term;
        private final int tf;
        private final int df;
        private final double tfIdfLog10;

        public TermStats(String term, int tf, int df, double tfIdfLog10) {
            this.term = term;
            this.tf = tf;
            this.df = df;
            this.tfIdfLog10 = tfIdfLog10;
        }

        public String getTerm() {
            return term;
        }

        public int getTf() {
            return tf;
        }

        public int getDf() {
            return df;
        }

        public double getTfIdfLog10() {
            return tfIdfLog10;
        }

        @Override
        public String toString() {
            return "Term: " + term + ", TF: " + tf + ", DF: " + df + ", TF x IDF(log10): " + tfIdfLog10;
        }
    }
}
