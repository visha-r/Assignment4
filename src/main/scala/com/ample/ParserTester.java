package com.ample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import com.scala.main.pageranker.Bz2WikiParser;


/**
 * Parser class responsible for parsing the given file containing html contents extracting the pageName and its outlinks only if valid.
 * 
 **/
public class ParserTester {
	public static void main(String[] args) throws IOException {
		BufferedReader reader = null;
		BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream("wikipedia-simple-html.bz2"));
        reader = new BufferedReader(new InputStreamReader(inputStream));
		
		//BufferedReader br = new BufferedReader(new FileReader(new File("wikipedia-simple-html.bz2")));
		String line = "";
		while ((line =reader.readLine()) != null) {
			System.out.println(com.scala.main.pageranker.Bz2WikiParser.parseLine(line));
		}
		System.out.println("Total nodes: " + Bz2WikiParser.count);
		reader.close();
	}
}

