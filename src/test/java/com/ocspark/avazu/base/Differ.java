package com.ocspark.avazu.base;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

public class Differ {

	public static void main(String[] args) {
		new Differ().diffLineByLine();
	}
	
	public Differ(){
		
	}
	
	public void diffLineByLine(){
		String file = "/home/bruce/workspace2/kaggle-avazu/base/tr.rx.app.new.csv";
		String file1 = "/home/bruce/workspace2/kaggle-avazu/base/tr.rx.app.new.1.csv";
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
		    String line;
		    BufferedReader br1 = new BufferedReader(new FileReader(file1));
		    String line1;
		    while ((line = br.readLine()) != null) {
		       line1 = br1.readLine();
		       String lineDiff = StringUtils.difference(line, line1);
		       System.out.println("lineDiff = " + lineDiff);
		    }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
