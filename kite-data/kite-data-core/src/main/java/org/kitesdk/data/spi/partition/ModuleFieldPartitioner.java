/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.partition;

import javax.annotation.Nullable;

import org.kitesdk.data.spi.FieldPartitioner;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;

public class ModuleFieldPartitioner extends FieldPartitioner<Long, Long> {

	private int module;

	public ModuleFieldPartitioner(String sourceName, int module) {
		this(sourceName, sourceName + "_part_mod_" + module, module);
	}

	public ModuleFieldPartitioner(String sourceName, String name, int module) {
		super(sourceName, name, Long.class, Long.class, module);

		this.module = module;
	}

	@Override
	public int compare(Long o1, Long o2) {
		return Long.compare(o1, o2);
	}

	@Override
	public Long apply(Long value) {
		return value % module;
	}

	@Override
	public Predicate<Long> project(Predicate<Long> predicate) {
		return null;
	}

	@Override
	public Predicate<Long> projectStrict(Predicate<Long> predicate) {
		return null;
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !getClass().equals(o.getClass())) {
			return false;
		}
		ModuleFieldPartitioner that = (ModuleFieldPartitioner) o;
		return Objects.equal(this.getSourceName(), that.getSourceName())
				&& Objects.equal(this.getName(), that.getName())
				&& Objects.equal(this.getCardinality(), that.getCardinality())
				&& Objects.equal(this.module, that.module);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(getSourceName(), getName(), getCardinality(), module);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("sourceName", getSourceName())
				.add("name", getName()).add("cardinality", getCardinality())
				.add("module", getModule()).toString();
	}
	
	public long getModule(){
		return module;
	}
}
