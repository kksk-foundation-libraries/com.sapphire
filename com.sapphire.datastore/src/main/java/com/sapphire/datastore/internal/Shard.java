package com.sapphire.datastore.internal;

import com.sapphire.datastore.Scheme;

class Shard {
	private String name;
	private Scheme scheme;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Scheme getScheme() {
		return scheme;
	}

	public void setScheme(Scheme scheme) {
		this.scheme = scheme;
	}
}
