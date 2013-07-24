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
package eu.stratosphere.nephele.streaming.util;

import java.util.Iterator;

/**
 * Convenient utility class to create a non-sparse iterable based on a sparse
 * iterator. A sparse iterator may return null values. This is achieved by
 * skipping the null values of the underlying iterator.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class SparseDelegateIterable<T> implements Iterable<T> {

	private Iterator<T> sparseIterator;

	public SparseDelegateIterable(Iterator<T> sparseIterator) {
		this.sparseIterator = sparseIterator;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {
			T current = null;

			@Override
			public boolean hasNext() {
				while (this.current == null
						&& SparseDelegateIterable.this.sparseIterator.hasNext()) {
					this.current = SparseDelegateIterable.this.sparseIterator
							.next();
				}
				return this.current != null;
			}

			@Override
			public T next() {
				T toReturn = this.current;
				this.current = null;
				return toReturn;
			}

			@Override
			public void remove() {
				SparseDelegateIterable.this.sparseIterator.remove();
			}
		};
	}
}
