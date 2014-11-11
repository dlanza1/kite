package org.kitesdk.data.spi.partition;

import javax.annotation.Nullable;

import org.kitesdk.data.spi.FieldPartitioner;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;

public class ModuleFieldPartitioner extends FieldPartitioner<Integer, Integer> {

	private int module;

	public ModuleFieldPartitioner(String sourceName, int module) {
		super(sourceName, sourceName + "_mod_" + module, Integer.class, Integer.class,
				(int) module);

		this.module = module;
	}

	@Override
	public int compare(Integer o1, Integer o2) {
		return Integer.compare(o1, o2);
	}

	@Override
	public Integer apply(Integer value) {
		return value % module;
	}

	@Override
	public Predicate<Integer> project(Predicate<Integer> predicate) {
		return null;
	}

	@Override
	public Predicate<Integer> projectStrict(Predicate<Integer> predicate) {
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
