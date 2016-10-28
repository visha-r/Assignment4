package com.scala.main.pageranker;

import java.util.LinkedList;
import java.util.List;

public class Node {
	double pageRank;
	List<String> adjacencyList;
	String pageName;
	
	Node() {
		this.pageName = "";
		this.adjacencyList = new LinkedList<>();
		this.pageRank = 0;
	}
	
	Node (String pageName, List<String> list) {
		this.pageName = pageName;
		this.adjacencyList = list;
	}
	
	public double getPageRank() {
		return pageRank;
	}
	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}
	public List<String> getAdjacencyList() {
		return adjacencyList;
	}
	public void setAdjacencyList(List<String> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public String getPageName() {
		return pageName;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}
	
	public String toString() {
		return this.pageName +"----" + this.adjacencyList.toString();
	}
}
