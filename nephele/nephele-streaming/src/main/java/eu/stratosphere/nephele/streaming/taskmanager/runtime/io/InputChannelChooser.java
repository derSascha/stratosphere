package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class is used by the {@link StreamInputGate} to efficiently determine in
 * which order to read from the input channels that currently have input data
 * available. It implements a round-robin schedule over a permanently changing
 * set of available input channels.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class InputChannelChooser {

	private final LinkedBlockingQueue<Integer> incomingAvailableChannels = new LinkedBlockingQueue<Integer>();

	private final RoundRobinChannelSchedule channelSchedule = new RoundRobinChannelSchedule();

	private volatile boolean blockIfNoChannelAvailable = true;

	/**
	 * @return index of the next available channel, or -1 if no channel is
	 *         currently available and blocking is switched off
	 * @throws InterruptedException
	 *             if thread is interrupted while waiting.
	 */
	public int chooseNextAvailableChannel() throws InterruptedException {
		this.dequeueIncomingAvailableChannels();

		if (this.channelSchedule.isEmpty()) {
			this.waitForAvailableChannelsIfNecessary();
		}

		return this.channelSchedule.nextChannel();
	}

	public void setBlockIfNoChannelAvailable(boolean blockIfNoChannelAvailable) {
		this.blockIfNoChannelAvailable = blockIfNoChannelAvailable;
		synchronized (this.incomingAvailableChannels) {
			// wake up any task thread that is waiting on available channels
			// so that it realizes it should be halted.
			this.incomingAvailableChannels.notify();
		}
	}

	public void markCurrentChannelUnavailable() {
		this.channelSchedule.unscheduleCurrentChannel();
	}

	/**
	 * If blocking is switched on, this method blocks until at least one channel
	 * is available, otherwise it may return earlier. If blocking is switched
	 * off while a thread waits in this method, it will return earlier as well.
	 * 
	 * @throws InterruptedException
	 */
	private void waitForAvailableChannelsIfNecessary()
			throws InterruptedException {
		synchronized (this.incomingAvailableChannels) {
			while (this.incomingAvailableChannels.isEmpty()
					&& this.blockIfNoChannelAvailable) {
				this.incomingAvailableChannels.wait();
			}
		}
		this.dequeueIncomingAvailableChannels();
	}

	public void addIncomingAvailableChannel(int channelIndex) {
		synchronized (this.incomingAvailableChannels) {
			this.incomingAvailableChannels.add(Integer.valueOf(channelIndex));
			this.incomingAvailableChannels.notify();
		}
	}

	private void dequeueIncomingAvailableChannels() {
		Integer incoming;
		while ((incoming = this.incomingAvailableChannels.poll()) != null) {
			this.channelSchedule.scheduleChannel(incoming);
		}
	}
}
