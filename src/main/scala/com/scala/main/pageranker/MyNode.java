package com.scala.main.pageranker;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Object representing a page holding its pageName, pageRank, adjacencyList & a boolean representing whether its a source node or an outlink node.
 * @author vjvsha
 *
 */
public class MyNode implements WritableComparable<MyNode> {
	
	DoubleWritable pageRank;
	Text adjacencyList;
	Text pageName;
	BooleanWritable isSourceNode;

	/**
	 * No argument constructor to initialize Node object
	 */
	MyNode() {
		this.pageRank = new DoubleWritable(); 
		this.adjacencyList = new Text("[]");
		this.pageName = new Text();
		this.isSourceNode = new BooleanWritable(false);
	}
	
	/**
	 * Initializes the value of this Node object to the given values
	 * 
	 * @param pageName
	 * @param pagerank
	 * @param adjacencyList
	 */
	MyNode(String pageName, double pagerank, String adjacencyList) {
		this.pageRank = new DoubleWritable(pagerank); 
		this.adjacencyList = new Text(adjacencyList);
		this.pageName = new Text(pageName);
		this.isSourceNode = new BooleanWritable(false);
	}
	
	/**
	 * Initializes the value of this Node object to the given values
	 * 
	 * @param pageName
	 * @param pagerank
	 * @param adjacencyList
	 */
	MyNode(String pageName, double pagerank) {
		this.pageRank = new DoubleWritable(pagerank); 
		this.pageName = new Text(pageName);
		this.adjacencyList = new Text("[]");
		this.isSourceNode = new BooleanWritable(false);
	}
	
	public void readFields(DataInput in) throws IOException {
		pageName.readFields(in);
		pageRank.readFields(in);
		adjacencyList.readFields(in);
		isSourceNode.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		pageName.write(out);
		pageRank.write(out);
		adjacencyList.write(out);
		isSourceNode.write(out);
	}
	
	/**
	 * @return the pageRank
	 */
	public double getPageRank() {
		return Double.valueOf(pageRank.toString());
	}
	
	/**
	 * @return the pageRank as Text
	 */
	public Text getTextPageRank() {
		return new Text(this.pageRank.toString());
	}
	
	/**
	 * @return the pageName
	 */
	public Text getTextPageName() {
		return new Text(this.getPageName());
	}
	
	/**
	 * @return the adjacencyList
	 */
	public String getAdjacencyList() {
		return adjacencyList.toString();
	}
	/**
	 * @param adjacencyList the adjacencyList to set
	 */
	public void setAdjacencyList(String adjacencyList) {
		this.adjacencyList = new Text(adjacencyList);
	}

	/**
	 * @return the pageName
	 */
	public String getPageName() {
		return pageName.toString();
	}

	/**
	 * @param pageName the pageName to set
	 */
	public void setPageName(String pageName) {
		this.pageName = new Text(pageName);
	}

	/**
	 * @param pageRank the pageRank to set
	 */
	public void setPageRank(double pageRank) {
		this.pageRank = new DoubleWritable(pageRank);
	}
	
	public void updatePageRank(double pageRank) {
		this.pageRank = new DoubleWritable(Double.valueOf(this.pageRank.toString()) + pageRank);
	}
	
	/**
	 * @param isSourceNode the boolean value for isSourceNode to set
	 */
	public void setIsSourceNode(boolean isSourceNode) {
		this.isSourceNode = new BooleanWritable(isSourceNode);
	}
	
	public boolean getIsSourceNode(){
		return this.isSourceNode.toString().equals("true");
	}
	
	public String toString() {
		return "/"+this.getAdjacencyList().trim()+"/"+ this.getTextPageRank();
	}
	
	/**
	 * Compares by stationId first and then by year
	 * 
	 */
	public int compareTo (MyNode other) {
		return this.pageName.compareTo(other.pageName);
	}
	
	/**
	 * Resets attribute values of Node object
	 * 
	 */
	public void reset() {
		this.adjacencyList.clear();
		this.pageName = new Text();
		this.pageRank = new DoubleWritable();
		this.isSourceNode = new BooleanWritable(false);
	}
	
	/**
	 * n1 & n2 are equal only their pageNames are equal
	 * 
	 * @param n1
	 * @param n2
	 * @return
	 */
	public boolean equals(MyNode n1, MyNode n2){
		return n1.getPageName().equals(n2.getPageName());
	}
	
	public int hashCode() {
        return this.getPageName().hashCode();
    }
}
