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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
//LSim logging system imports sgeag@2017
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.worker.LSimWorker;
import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias December 2012
 *
 */
public class Log implements Serializable {
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations received by a client.
	 * They are stored in a ConcurrentHashMap (a hash table), that stores a list of
	 * operations for each member of the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log = new ConcurrentHashMap<String, List<Operation>>();

	public Log(List<String> participants) {
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext();) {
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are inserted in order. If the
	 * last operation for the user is not the previous operation than the one being
	 * inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op) {
		lsim.log(Level.TRACE, "Inserting into Log the operation: " + op);

		/**
		 * Getting the hostId and current timestamp to search the last submitted
		 * operation in order to then check whether the new operation is newer or not
		 */
		String hostId = op.getTimestamp().getHostid();
		Timestamp lastTimestamp = this.getLastTimestamp(hostId);
		long timestampDifference = op.getTimestamp().compare(lastTimestamp);

		/**
		 * Check if the inserted Operation is the next to follow. If yes, insert it and
		 * return true so it can be purged eventually later, otherwise return false so
		 * that it is kept for later.
		 */

		if ((lastTimestamp == null && timestampDifference == 0)
				|| (lastTimestamp != null && timestampDifference == 1)) {
			log.get(hostId).add(op);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Checks the received summary (sum) and determines the operations contained in
	 * the log that have not been seen by the proprietary of the summary. Returns
	 * them in an ordered list.
	 * 
	 * @param sum
	 * @return list of operations
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum) {

		// Will store newer operations that are yet to be synced
		List<Operation> nonAddedOps = new Vector<Operation>();

		/**
		 * While iterating trough all the hosts available in the log, compare operation
		 * timestamps between the last logged op on the current host and the last logged
		 * op on the summary for that host. Any operation timestamp is smaller on the
		 * current host it will need to be updated from the summary and therefore will
		 * be added to the nonAdded list.
		 */

		for (String node : log.keySet()) {
			List<Operation> ops = log.get(node);
			Timestamp lasHostTimestampOnSum = sum.getLast(node);

			for (Operation op : ops) {
				if (op.getTimestamp().compare(lasHostTimestampOnSum) > 1) {
					nonAddedOps.add(op);
				}
			}
		}

		return nonAddedOps;
	}

	/**
	 * Removes from the log the operations that have been acknowledged by all the
	 * members of the group, according to the provided ackSummary.
	 * 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack) {
	}

	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		Log other = (Log) obj;
		if (log == null) {
			return other.log == null;
		} else {
			if (log.size() != other.log.size()) {
				return false;
			}
			boolean equal = true;
			for (Iterator<String> it = log.keySet().iterator(); it.hasNext() && equal;) {
				String hostName = it.next();
				equal = log.get(hostName).equals(other.log.get(hostName));
			}

			return equal;
		}
	}

	private Timestamp getLastTimestamp(String hostId) {
		List<Operation> operations = log.get(hostId);

		if (operations == null || operations.isEmpty()) {
			return null;
		} else {
			return operations.get(operations.size() - 1).getTimestamp();
		}
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name = "";
		for (Enumeration<List<Operation>> en = log.elements(); en.hasMoreElements();) {
			List<Operation> sublog = en.nextElement();
			for (ListIterator<Operation> en2 = sublog.listIterator(); en2.hasNext();) {
				name += en2.next().toString() + "\n";
			}
		}
		return name;
	}
}