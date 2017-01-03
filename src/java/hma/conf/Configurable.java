package hma.conf;

import hma.conf.Configuration;

/** Something that may be configured with a {@link Configuration}. */
public interface Configurable {

	/** Set the configuration to be used by this object. */
	void setConf(Configuration conf);

	/** Return the configuration used by this object. */
	Configuration getConf();
}
