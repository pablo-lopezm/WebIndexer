package es.udc.fic.ri;


import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.*;

public class TopTermsInField {
    public static void main(String[] args){
        String usage =
                "java es.udc.fic.ri.TopTermsInField"
                        + " [-index INDEX_PATH] [-field FIELD] [-outfile OUTFILE_PATH] [-top TOP]\n\n"
                        + "This finds the top TOP terms for a field FIELD in index INDEX_PATH "
                        + "and stores the results in OUTFILE_PATH\n";
        String indexPath= null;
        String field = null;
        String outfile = null;
        int top = 10;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                case "-top":
                    top = Integer.parseInt(args[++i]);
                    break;
                case "-outfile":
                    outfile = args[++i];
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.exit(1);
            }
        }

        if (indexPath == null || field == null || outfile == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        try {
            Directory dir = FSDirectory.open(Paths.get(indexPath));
            DirectoryReader indexReader = DirectoryReader.open(dir);
            System.out.println("Top " + top + " terms for field " + field + ":\n");
            if(indexReader == null) System.exit(1);
            final Terms terms = MultiTerms.getTerms(indexReader, field);

            Map<String, Integer> termDFMap = new HashMap<>();


            if (terms != null) {
                final TermsEnum termsEnum = terms.iterator();

                while (termsEnum.next() != null) {
                    String termText = termsEnum.term().utf8ToString();
                    int docFreq = termsEnum.docFreq();
                    termDFMap.put(termText, docFreq);
                }
            }

            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outfile)))) {
                termDFMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(top)
                        .forEach(entry -> {
                            System.out.println(entry.getKey() + ": " + entry.getValue());
                            writer.println(entry.getKey() + ": " + entry.getValue());
                        });
            } catch (IOException e) {
                System.out.println("Cannot write to output file: exception " + e);
                e.printStackTrace();
            }
            indexReader.close();
            dir.close();
        } catch (IOException e) {
                System.out.println("Error reading indexes: exception " + e);
                e.printStackTrace();
        }
    }
}
