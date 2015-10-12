package com.thinkbiganalytics.hadoop.sas;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.eobjects.metamodel.sas.SasColumnType;

public class SasDefaultCallback implements SasInMemoryReaderCallback{

	private List<SasColumnModel> sasColumnModelList = new LinkedList<SasColumnModel>();
	
	private Queue<Object[]> resultObjectStack = new LinkedList<Object[]>();
	
	public void column(int columnIndex, String columnName, String columnLabel, SasColumnType columnType, int columnLength) {
		sasColumnModelList.add(new SasColumnModel(columnIndex, columnName, columnLabel, columnType, columnLength));
	}

	public boolean readData() {
		return true;
	}

	public boolean row(int rowIndex, Object[] rowObjects) {
		if(resultObjectStack.isEmpty()){
			resultObjectStack.add(getHeaders());
		}
		resultObjectStack.add(rowObjects);
		return true;
	}
	
	private Object[] getHeaders(){
		Object[] headers = new Object[sasColumnModelList.size()];
		for(int i = 0;i<sasColumnModelList.size();i++){
			headers[i] = sasColumnModelList.get(i).toString();
		}
		return headers;
	}

	public List<SasColumnModel> getSasColumnModelList() {
		return sasColumnModelList;
	}

	public Queue<Object[]> getResultObjectStack() {
		return resultObjectStack;
	}

}
