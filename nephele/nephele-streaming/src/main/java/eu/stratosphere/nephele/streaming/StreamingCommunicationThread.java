/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.streaming;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.PluginCommunicationProtocol;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class implements a communication thread to handle communication from the
 * task manager plugin component to the job manager plugin component in an
 * asynchronous fashion. The main reason for asynchronous communication is not
 * influence the processing delay by the RPC call latency.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class StreamingCommunicationThread extends Thread {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory
			.getLog(StreamingCommunicationThread.class);

	/**
	 * The blocking queue which is used to asynchronously exchange data with the
	 * job manager component of this plugin.
	 */
	private final BlockingQueue<AbstractStreamingData> dataQueue = new LinkedBlockingQueue<AbstractStreamingData>();

	private final BlockingQueue<InstanceConnectionInfo> connectionInfoQueue = new LinkedBlockingQueue<InstanceConnectionInfo>();

	private HashMap<InstanceConnectionInfo, PluginCommunicationProtocol> proxies = new HashMap<InstanceConnectionInfo, PluginCommunicationProtocol>();

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {

		try {
			while (!interrupted()) {
				InstanceConnectionInfo connectionInfo = this.connectionInfoQueue
						.take();
				AbstractStreamingData data = this.dataQueue.take();

				try {
					this.getProxy(connectionInfo).sendData(
							StreamingPluginLoader.STREAMING_PLUGIN_ID, data);
				} catch (IOException ioe) {
					LOG.error(StringUtils.stringifyException(ioe));
					this.proxies.remove(connectionInfo);
				}
			}
		} catch (InterruptedException e) {
		}
	}

	private PluginCommunicationProtocol getProxy(
			InstanceConnectionInfo connectionInfo) throws IOException {
		PluginCommunicationProtocol proxy = this.proxies.get(connectionInfo);
		if (proxy == null) {
			proxy = RPC.getProxy(PluginCommunicationProtocol.class,
					new InetSocketAddress(connectionInfo.getAddress(),
							connectionInfo.getIPCPort()), NetUtils
							.getSocketFactory());
			this.proxies.put(connectionInfo, proxy);
		}

		return proxy;
	}

	/**
	 * Stops the communication thread.
	 */
	void stopCommunicationThread() {
		this.interrupt();
	}

	public void sendToTaskManagerAsynchronously(
			final InstanceConnectionInfo connectionInfo,
			final AbstractStreamingData data) throws InterruptedException {
		this.dataQueue.put(data);
		this.connectionInfoQueue.put(connectionInfo);
	}
}
