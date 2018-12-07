/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

//LSim logging system imports sgeag@2017
import lsim.worker.LSimWorker;
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}
	
	 private TimestampVector(Map<String, Timestamp> timestampVector) {
	        this.timestampVector = new ConcurrentHashMap<>(timestampVector);
	    }
	
	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public synchronized void updateTimestamp(Timestamp timestamp){
		lsim.log(Level.TRACE, "Updating the TimestampVectorInserting with the timestamp: "+timestamp);

	
	    this.timestampVector.replace(timestamp.getHostid(), timestamp);
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector){
		if (tsVector == null) {
            return;
        }

		for (String node : this.timestampVector.keySet()) {
            Timestamp lastLoggedOpForNode = tsVector.getLast(node);
            
            if(lastLoggedOpForNode == null) {
            	continue;
            } else  if (this.getLast(node).compare(lastLoggedOpForNode) < 0) {
                this.timestampVector.replace(node, lastLoggedOpForNode);
            }
        }
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public synchronized Timestamp getLast(String node){
		return this.timestampVector.get(node);
	}
	
	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public void mergeMin(TimestampVector tsVector){
		 return;
	}
	
	/**
	 * clone
	 */

	public synchronized TimestampVector clone(){
		return new TimestampVector(this.timestampVector);	}
	
	
    public synchronized boolean equals(Object vector) {
    	 if (vector == null) {
             return false;
         } else if (this == vector) {
             return true;
         } else if (!(vector instanceof TimestampVector)) {
             return false;
         }

         return equals((TimestampVector) vector);
    }
    
    /**
	 * equals
	 */
    public synchronized boolean equals(TimestampVector otherVector) {
        if (this.timestampVector == otherVector.timestampVector) {
            return true;
        } else if (this.timestampVector == null || otherVector.timestampVector == null) {
            return false;
        } else {
            return this.timestampVector.equals(otherVector.timestampVector);
        }
    }

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
		}
		return all;
	}
}