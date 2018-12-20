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
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

//LSim logging system imports sgeag@2017
import edu.uoc.dpcs.lsim.LSimFactory;
import lsim.worker.LSimWorker;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias December 2012
 *
 */
public class TimestampMatrix implements Serializable {
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();

	public TimestampMatrix(List<String> participants) {
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext();) {
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}

	/**
	 * Not private for testing purposes.
	 * 
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node) {

		// return generated automatically. Remove it when implementing your solution
		return null;
	}

	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * 
	 * @param tsMatrix
	 */
	public void updateMax(TimestampMatrix tsMatrix) {
		for (Map.Entry<String, TimestampVector> entry : tsMatrix.timestampMatrix.entrySet()) {
			String key = entry.getKey();
			TimestampVector otherValue = entry.getValue();

			TimestampVector thisValue = this.timestampMatrix.get(key);
			if (thisValue != null) {
				thisValue.updateMax(otherValue);
			}
		}
	}

	/**
	 * substitutes current timestamp vector of node for tsVector
	 * 
	 * @param node
	 * @param tsVector
	 */
	public void update(String node, TimestampVector tsVector) {
		this.timestampMatrix.replace(node, tsVector);
	}

	/**
	 * 
	 * @return a timestamp vector containing, for each node, the timestamp known by
	 *         all participants
	 */
	public TimestampVector minTimestampVector() {
		TimestampVector minTsV = null;

		for (TimestampVector matrixVector : this.timestampMatrix.values()) {
			if (minTsV == null)
				minTsV = matrixVector.clone();
			else
				minTsV.mergeMin(matrixVector);
		}

		return minTsV;
	}

	/**
	 * clone
	 */
	private TimestampMatrix() {
	}

	public TimestampMatrix clone() {

		TimestampMatrix clonedMatrix = new TimestampMatrix();

		for (Map.Entry<String, TimestampVector> entry : timestampMatrix.entrySet()) {
			clonedMatrix.timestampMatrix.put(entry.getKey(), entry.getValue().clone());
		}

		return clonedMatrix;

	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {

		if (obj == null) {
			return false;
		} else if (this == obj) {
			return true;
		} else if (!(obj instanceof TimestampMatrix)) {
			return false;
		}

		TimestampMatrix other = (TimestampMatrix) obj;

		if (this.timestampMatrix == other.timestampMatrix) {
			return true;
		} else if (this.timestampMatrix == null || other.timestampMatrix == null) {
			return false;
		} else {
			return this.timestampMatrix.equals(other.timestampMatrix);
		}

	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all = "";
		if (timestampMatrix == null) {
			return all;
		}
		for (Enumeration<String> en = timestampMatrix.keys(); en.hasMoreElements();) {
			String name = en.nextElement();
			if (timestampMatrix.get(name) != null)
				all += name + ":   " + timestampMatrix.get(name) + "\n";
		}
		return all;
	}
}