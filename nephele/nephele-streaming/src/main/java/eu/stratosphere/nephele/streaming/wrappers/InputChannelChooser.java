package eu.stratosphere.nephele.streaming.wrappers;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;

public class InputChannelChooser {

	private final LinkedBlockingQueue<Integer> incomingAvailableChannels = new LinkedBlockingQueue<Integer>();

	private final HashSet<Integer> availableChannels = new HashSet<Integer>();

	private LinkedList<Integer> doneChannels = new LinkedList<Integer>();

	private LinkedList<Integer> pendingChannels = new LinkedList<Integer>();

	private volatile boolean blockIfNoChannelAvailable = true;

	/**
	 * @return index of the next available channel, or -1 if no channel is
	 *         currently available and blocking is switched off
	 * @throws InterruptedException
	 *             if thread is interrupted while waiting.
	 */
	public int chooseNextAvailableChannel() throws InterruptedException {
		this.dequeueIncomingAvailableChannels();

		if (this.availableChannels.isEmpty()) {
			this.waitForAvailableChannelsIfNecessary();
		}

		return this.tryToDequeueNextChannel();
	}

	private int tryToDequeueNextChannel() {
		if (this.pendingChannels.isEmpty()) {
			this.switchDoneAndPendingChannels();
		}

		int currentChannel = -1;
		if (!this.pendingChannels.isEmpty()) {
			currentChannel = this.pendingChannels.removeFirst();
			this.doneChannels.addLast(currentChannel);
		}

		return currentChannel;
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
		int currentChannel = this.doneChannels.removeLast();
		this.availableChannels.remove(currentChannel);
	}

	private void switchDoneAndPendingChannels() {
		LinkedList<Integer> tmp = this.doneChannels;
		this.doneChannels = this.pendingChannels;
		this.pendingChannels = tmp;
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
			this.dequeueIncomingAvailableChannels();
			while (this.incomingAvailableChannels.isEmpty()
					&& this.blockIfNoChannelAvailable) {
				this.incomingAvailableChannels.wait();
			}
		}
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
			this.setChannelAvailable(incoming);
		}
	}

	private void setChannelAvailable(int channelIndex) {
		boolean added = this.availableChannels.add(channelIndex);
		if (added) {
			this.pendingChannels.add(channelIndex);
		}
	}

}
