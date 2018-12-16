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

package recipes_service.tsae.sessions;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import recipes_service.ServerData;
import java.util.ArrayList;

import recipes_service.data.*;
import recipes_service.tsae.data_structures.*;
import recipes_service.communication.*;

//LSim logging system imports sgeag@2017
import lsim.worker.LSimWorker;
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread {
	// Needed for the logging system sgeag@2017
	private LSimWorker lsim = LSimFactory.getWorkerInstance();

	private Socket socket = null;
	private ServerData serverData = null;

	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {

		Message msg = null;

		int current_session_number = -1;
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			/**
			 * Receive originator's summary and ack
			 */
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();
			lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] TSAE session");
			lsim.log(Level.TRACE,
					"[TSAESessionPartnerSide] [session: " + current_session_number + "] received message: " + msg);
			if (msg.type() == MsgType.AE_REQUEST) {
				MessageAErequest aeRequestMsg = (MessageAErequest) msg;
				TimestampVector partnerSummary = aeRequestMsg.getSummary();
				Log localLog = serverData.getLog();
				List<Operation> newOperations = localLog.listNewer(partnerSummary);

				TimestampVector localSummary = null;
				TimestampMatrix localAck = null;

				/**
				 * Store local summary and ack. This needs to be synchronized because we must
				 * ensure that nothing is modified in the meantime
				 */
				synchronized (serverData) {
					localSummary = serverData.getSummary().clone();
				}

				/**
				 * Send local operations that the partner hasn't acknowledged yet (acording to
				 * their summary).
				 */
				for (Operation op : newOperations) {
					out.writeObject(new MessageOperation(op));
				}

				// send to originator: local's summary and ack

				msg.setSessionNumber(current_session_number);
				lsim.log(Level.TRACE,
						"[TSAESessionPartnerSide] [session: " + current_session_number + "] sent message: " + msg);

				/**
				 * Send summary and ack to partner (request Anti entropy session)
				 */
				msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(current_session_number);
				out.writeObject(msg);
				lsim.log(Level.TRACE,
						"[TSAESessionPartnerSide] [session: " + current_session_number + "] sent message: " + msg);

				// receive operations
				List<MessageOperation> operations = new ArrayList<MessageOperation>(); // This will store operations
																						// from the partner
				/**
				 * Recieve operations from partner
				 */
				msg = (Message) in.readObject();
				/**
				 * Store operations that aren't yet acknowledged by local host.
				 */
				lsim.log(Level.TRACE,
						"[TSAESessionPartnerSide] [session: " + current_session_number + "] received message: " + msg);
				while (msg.type() == MsgType.OPERATION) {
					operations.add((MessageOperation) msg);

					msg = (Message) in.readObject();
					lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number
							+ "] received message: " + msg);
				}

				/**
				 * Received message about the ending of the TSAE session
				 */
				if (msg.type() == MsgType.END_TSAE) {
					msg = new MessageEndTSAE();
					out.writeObject(msg);

					/**
					 * If we're here then our session was successful and we can update the data
					 * locally
					 */
					synchronized (serverData) {
						for (MessageOperation op : operations) {
							AddOperation opEl = (AddOperation) op.getOperation();
							OperationType opType = opEl.getType();
							if (opType == OperationType.ADD) {
								if (localLog.add(opEl)) {
									Recipe recipe = opEl.getRecipe();
									serverData.getRecipes().add(recipe);
								}
							}

						}

						serverData.getSummary().updateMax(aeRequestMsg.getSummary());
					}

					msg.setSessionNumber(current_session_number);
					lsim.log(Level.TRACE,
							"[TSAESessionPartnerSide] [session: " + current_session_number + "] sent message: " + msg);
				}

			}
			socket.close();
		} catch (ClassNotFoundException e) {
			lsim.log(Level.FATAL,
					"[TSAESessionPartnerSide] [session: " + current_session_number + "]" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
		}

		lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] End TSAE session");
	}
}