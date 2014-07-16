package com.hazelcast.lucene.directory;

import com.hazelcast.core.Hazelcast;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.Stack;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;
import javax.swing.table.DefaultTableModel;

/**
 * Created by mesutcelik on 7/16/14.
 */
public class HazelcastLuceneSearcher {

    public static long sizeInTotal;
    public static Directory dir;
    public static Analyzer analyzer;
    public static QueryParser parser;
    public static IndexReader reader;
    public static IndexSearcher searcher;
    
    public static void main(String[] args) throws Exception {

    	init();
    	
    	UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
    	
    	final JFrame frame = new JFrame();
    	
    	JLabel lblSearch = new JLabel("Word to search: ");
    	final JTextField txtSearch = new JTextField();
    	txtSearch.setColumns(20);
    	JButton btnSearch = new JButton("Search");
    	
    	JPanel pnlNorth = new JPanel();
    	pnlNorth.add(lblSearch);
    	pnlNorth.add(txtSearch);
    	pnlNorth.add(btnSearch);
    	
    	final JPanel pnlCenter = new JPanel();
    	
    	final JTable tblSearchResult = new JTable();
    
    	tblSearchResult.setSize(400, 400);
    	
    	final JTextArea txtSearchResult = new JTextArea();
    	
    	btnSearch.addActionListener(new ActionListener() {
			
			public void actionPerformed(ActionEvent e) {
				String[] columnNames = {"First Name",
		                "Last Name",
		                "Sport",
		                "# of Years",
		                "Vegetarian"};
		    	
		    	
				Object[][] data;
				try {
					data = search(txtSearch.getText());
					StringBuilder sb = new StringBuilder();
					DefaultTableModel model = (DefaultTableModel)tblSearchResult.getModel();
					for (int i = 0; i < data.length; i++) {
						System.out.println(i);
						model.addRow(data[i]);
						sb.append(data[i][0] + ": " + data[i][1]).append("\n");
					}
					model.fireTableDataChanged();
					tblSearchResult.repaint();
					
					txtSearchResult.setText(sb.toString());
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    	
			}
		});
    	
    	
    	//tblSearchResult.setSize(450, 300);
    	//pnlCenter.add(comp)
    	pnlCenter.add(tblSearchResult);
    	
    	frame.setLayout(new BorderLayout());
    	frame.add(pnlNorth, BorderLayout.NORTH);
    	frame.add(txtSearchResult, BorderLayout.CENTER);
    	frame.setSize(1000, 400);
    	frame.setResizable(true);
    	frame.setLocationRelativeTo(null);
    	frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    	frame.setVisible(true);
    	
    	/*
        String docsPath = "/Users/serkanozal/Documents/workspace";

        final File docDir = new File(docsPath);
        Date start = new Date();
        try {

            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_46, analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

            Directory dir = new HazelcastDirectory();

            IndexWriter writer = new IndexWriter(dir, iwc);
            //  for (int i = 0; i < 40; i++) {

            indexDocs(writer, docDir, 0);
            //}
            writer.close();

            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds for Indexing");


            String field = "contents";
            String queries = null;
            int repeat = 0;
            boolean raw = false;
            String queryString = null;
            int hitsPerPage = 10000;


            IndexReader reader = DirectoryReader.open(dir);
            IndexSearcher searcher = new IndexSearcher(reader);

            BufferedReader in = null;
            if (queries != null) {
                in = new BufferedReader(new InputStreamReader(new FileInputStream(queries), "UTF-8"));
            } else {
                in = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
            }
            QueryParser parser = new QueryParser(Version.LUCENE_46, field, analyzer);

            start = new Date();

            Stack<String> s = new Stack<String>();
            s.push("HazelcastDirectoryApp");

            while (!s.empty()) {
                Query query = parser.parse(s.pop());
                ScoreDoc[] docs = searcher.search(query,100).scoreDocs;
                for (ScoreDoc doc : docs) {
                    Document doc1 = searcher.doc(doc.doc);
                    System.out.println("path: "+doc1.get("path"));
                    System.out.println("line " + doc1.get("lineNo") + ": " + doc1.get("contents"));
                }
                Thread.sleep(100);
            }
            end = new Date();
            reader.close();
            dir.close();

        } catch (Exception e) {
            System.out.println(" caught a " + e.getClass() +
                    "\n with message: " + e.getMessage());
        }

        Hazelcast.shutdownAll();
*/
    }

    private static void init() {
    	try {
    		analyzer = new StandardAnalyzer(Version.LUCENE_46);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_46, analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

           dir = new HazelcastDirectory();

            IndexWriter writer = new IndexWriter(dir, iwc);
 
	    	parser = new QueryParser(Version.LUCENE_46, "contents", analyzer);
	    	reader = DirectoryReader.open(dir);
	        searcher = new IndexSearcher(reader);
    	} catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static Object[][] search(String textToSearch) throws Exception {
    	Object[][] data = null;
    	Stack<String> s = new Stack<String>();
    	for (String text : textToSearch.split("//s+")) {
    		s.push(text);
    	}
        while (!s.empty()) {
            Query query = parser.parse(s.pop());
            ScoreDoc[] docs = searcher.search(query,100).scoreDocs;
            data = new Object[docs.length][];
            int i = 0;
            for (ScoreDoc doc : docs) {
            	data[i] = new Object[2];
                Document doc1 = searcher.doc(doc.doc);
                data[i][0] = doc1.get("path");
                data[i][1] = doc1.get("lineNo");
                System.out.println("path: "+doc1.get("path"));
                System.out.println("line " + doc1.get("lineNo") + ": " + doc1.get("contents"));
                i++;
            }
            Thread.sleep(100);
        }
    	
    	return data;
    }
    
    
    static void indexDocs(IndexWriter writer, File file, Integer order)
            throws IOException {
        // do not try to index files that cannot be read
        if (file.canRead()) {
            if (file.isDirectory()) {
                String[] files = file.list();
                // an IO error could occur
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        indexDocs(writer, new File(file, files[i]), order);
                    }
                }
            } else {
                if ((!file.getName().contains(".java") && !file.getName().contains(".xml")))
                    return;
                FileInputStream fis;
                try {
                    fis = new FileInputStream(file);
                } catch (FileNotFoundException fnfe) {
                    // at least on windows, some temporary files raise this exception with an "access denied" message
                    // checking if the file can be read doesn't help
                    return;
                }

                try {

                    

                    // Add the contents of the file to a field named "contents".  Specify a Reader,
                    // so that the text of the file is tokenized and indexed, but not stored.
                    // Note that FileReader expects the file to be in UTF-8 encoding.
                    // If that's not the case searching for special characters will fail.
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
                    int lineNo = 1; 
                    for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                    	// make a new, empty document
                        Document doc = new Document();

                        // Add the path of the file as a field named "path".  Use a
                        // field that is indexed (i.e. searchable), but don't tokenize
                        // the field into separate words and don't index term frequency
                        // or positional information:

                        Field pathField = new StringField("path", file.getPath(), Field.Store.YES);
                        doc.add(pathField);

                        // Add the last modified date of the file a field named "modified".
                        // Use a LongField that is indexed (i.e. efficiently filterable with
                        // NumericRangeFilter).  This indexes to milli-second resolution, which
                        // is often too fine.  You could instead create a number based on
                        // year/month/day/hour/minutes/seconds, down the resolution you require.
                        // For example the long value 2011021714 would mean
                        // February 17, 2011, 2-3 PM.
                        doc.add(new LongField("modified", file.lastModified(), Field.Store.NO));
                        
                    	doc.add(new TextField("contents", line, Field.Store.YES));
                    	doc.add(new IntField("lineNo", lineNo++, Field.Store.YES));
                    	
                    	if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                            writer.addDocument(doc);
                        } else {
                            writer.updateDocument(new Term("path", file.getPath()), doc);
                        }
                    }
                    if (writer.getConfig().getOpenMode() == IndexWriterConfig.OpenMode.CREATE) {
                        // New index, so we just add the document (no old document can be there):
//                        if (file.getName().contains(".pack") || file.getName().contains(".idx"))
//                            return;
                        sizeInTotal += file.length();
                        //System.out.println("adding " + file.getName() + "size:" + file.length());
                    } else {
                        // Existing index (an old copy of this document may have been indexed) so
                        // we use updateDocument instead to replace the old one matching the exact
                        // path, if present:
                    	//System.out.println("updating " + file);
                    }

                } finally {
                    fis.close();
                }
            }
        }
    }


    public static String readableFileSize(long size) {
        if (size <= 0) return "0";
        final String[] units = new String[]{"B", "KB", "MB", "GB", "TB"};
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

}
