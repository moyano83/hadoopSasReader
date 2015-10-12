package com.thinkbiganalytics.hadoop.sas;

import org.eobjects.metamodel.sas.SasColumnType;

public class SasColumnModel{
	@Override
	public String toString() {
		return columnName.concat("(").concat(columnType.name()).concat(")");
	}
	/** columnIndex the index (0-based) of the column */
	private int columnIndex;
	/** columnName the physical name of the column */
	private String columnName;
	/** columnLabel the logical label of the column (often more user-friendly than name) */
	private String columnLabel;
	/** columnType the type of the column */
	private SasColumnType columnType;
	/** columnLength the length of the column */
	private int columnLength;
	/**
	 * Constructor of the class
	 * @param columnIndex the index (0-based) of the column
	 * @param columnName the physical name of the column
	 * @param columnLabel the logical label of the column (often more user-friendly than name)
	 * @param columnType the type of the column
	 * @param columnLength the length of the column
	 */
	public SasColumnModel(int columnIndex, String columnName, String columnLabel, SasColumnType columnType, int columnLength){
		this.columnIndex = columnIndex;
		this.columnName = columnName;
		this.columnLabel = columnLabel;
		this.columnType = columnType;
		this.columnLength = columnLength;
	}
	/**
	 * @return the columnIndex
	 */
	public int getColumnIndex() {
		return columnIndex;
	}
	/**
	 * @param columnIndex the columnIndex to set
	 */
	public void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}
	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}
	/**
	 * @param columnName the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	/**
	 * @return the columnLabel
	 */
	public String getColumnLabel() {
		return columnLabel;
	}
	/**
	 * @param columnLabel the columnLabel to set
	 */
	public void setColumnLabel(String columnLabel) {
		this.columnLabel = columnLabel;
	}
	/**
	 * @return the columnType
	 */
	public SasColumnType getColumnType() {
		return columnType;
	}
	/**
	 * @param columnType the columnType to set
	 */
	public void setColumnType(SasColumnType columnType) {
		this.columnType = columnType;
	}
	/**
	 * @return the columnLength
	 */
	public int getColumnLength() {
		return columnLength;
	}
	/**
	 * @param columnLength the columnLength to set
	 */
	public void setColumnLength(int columnLength) {
		this.columnLength = columnLength;
	}
}
