package eu.stratosphere.nephele.streaming.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * Maps {@link AbstractID} objects (16 byte) to integer objects (4 byte) for the
 * purpose of compressing message sizes. Objects of this class are either in
 * READBLE or WRITABLE mode. In READABLE mode you first have to populate the ID
 * map by calling {@link #read(DataInput)} and can then retrieve
 * {@link AbstractID} objects by calling {@link #getFullID(int)}. In WRITABLE
 * mode, you can populate the ID map by repeatedly calling
 * {@link #getIntID(AbstractID)} and then serialize the map for shipment by
 * calling {@link #write(DataOutput)}.
 * 
 * @author Bjoern Lohrmann
 * @param <T>
 */
public class IdMapper<T extends AbstractID> implements IOReadableWritable {

	/**
	 * Maps 16 byte {@link AbstractID} objects to an integer unique within this
	 * message. This is an optimization so that each ID object only has to be
	 * transferred once within this message.
	 */
	private HashMap<T, Integer> idMap;

	/**
	 * Contains all {@link AbstractID} objects that have been mapped to an
	 * integer ID exactly in the order they were mapped.
	 */
	private ArrayList<T> mappedIDs;

	/**
	 * Next free integer ID to be used in {@link #idMap}. Starts at 0.
	 */
	private int nextFreeID;

	private MapperMode mode;

	private IDFactory<T> idFactory;

	public enum MapperMode {
		READABLE, WRITABLE
	}

	public interface IDFactory<T> {
		public T read(DataInput in) throws IOException;
	}

	/**
	 * Initializes a new IdMapper object depending on the given {{@link #mode}.
	 * 
	 * @param mode
	 *            If WRITABLE, then {@link #idFactory} will be ignored,
	 *            otherwise it will be used to create new ID objects during
	 *            deserialization.
	 * @param idFactory
	 *            Used to create new ID objects.
	 */
	public IdMapper(MapperMode mode, IDFactory<T> idFactory) {
		this.mode = mode;

		if (this.mode == MapperMode.WRITABLE) {
			this.idMap = new HashMap<T, Integer>();
			this.mappedIDs = new ArrayList<T>();
			this.nextFreeID = 0;
		} else {
			this.idFactory = idFactory;
		}
	}

	public int getIntID(T abstractID) {
		Integer intID = this.idMap.get(abstractID);
		if (intID == null) {
			intID = this.nextFreeID;
			this.nextFreeID++;
			this.idMap.put(abstractID, intID);
			this.mappedIDs.add(abstractID);
		}

		return intID;
	}

	public T getFullID(int integerID) {
		return this.mappedIDs.get(integerID);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.mappedIDs.size());
		for (T id : this.mappedIDs) {
			id.write(out);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		int idCount = in.readInt();
		this.mappedIDs = new ArrayList<T>(idCount);
		for (int i = 0; i < idCount; i++) {
			this.mappedIDs.add(this.idFactory.read(in));
		}
	}

	public boolean isEmpty() {
		return this.mappedIDs.isEmpty();
	}
}
