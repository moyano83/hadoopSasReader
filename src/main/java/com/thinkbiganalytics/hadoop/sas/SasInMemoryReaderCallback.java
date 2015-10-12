package com.thinkbiganalytics.hadoop.sas;

import java.util.Queue;

import org.eobjects.metamodel.sas.SasReaderCallback;

public interface SasInMemoryReaderCallback extends SasReaderCallback{
	/**
	 * @return the resultObjectList
	 */
	Queue<Object[]> getResultObjectStack();
}
