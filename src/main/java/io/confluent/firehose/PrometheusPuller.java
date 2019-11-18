package io.confluent.firehose;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;

import io.confluent.config.ConfigUtils;
import io.prometheus.client.exporter.HTTPServer;

/**
 * Sample Implementation that exposes the Confluent Cloud Metrics Firehose to Prometheus 
 * using the default Pull based methodology. 
 * 
 * @author Nikoleta Verbeck
 *
 */
public class PrometheusPuller extends AbstractPrometheusFirehose {
	private static final Logger LOG = LoggerFactory.getLogger(PrometheusPuller.class);
	public static final Options options = new Options();
	static {
		options.addOption("c", "config", true, "Comma Seperated key=value pairs of configs");
		options.addOption("f", "config-file", true, "Configs from a file");
		options.addOption("h", "help", false, "Print Help");
	}
	
	protected HTTPServer server;

	protected PrometheusPuller(Configuration config) {
		super(config);
	}
	
	@Override
	public void start() {
		try {
			
			final String listener = config.getString(Configs.CONFIG_PROM_PULLER_LISTENER);
			final String[] parts = listener.split(":");
			LOG.info("Starting Prometheus Webapp on {}", listener);
			final InetSocketAddress address = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
			server = new HTTPServer(address, this.registry);
		} catch (IOException e) {
			LOG.error("Failed to start Prometheus Web App", e);
			printHelp();
			System.exit(1);
		}
		
		super.start();
	}
	
	@Override
	public void close() throws IOException {
		super.close();
		if(server != null) {
			server.stop();
		}
	}

	@Override
	protected void endOfSet() throws Exception {
		
	}
	
	protected static void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(PrometheusPusher.class.getSimpleName(), options);
	}

	public static void main(String[] args) throws Exception {
		// create the parser
		final CommandLineParser parser = new DefaultParser();
		try {
			// parse the command line arguments
			final CommandLine line = parser.parse(options, args);
			if (line.hasOption('h')) {
				printHelp();
				System.exit(0);
			}
			
			final EnvironmentConfiguration envConfig = new EnvironmentConfiguration();

			final CompositeConfiguration configs = new CompositeConfiguration();
			configs.addConfiguration(new SystemConfiguration());
			configs.addConfiguration(ConfigUtils.envToProp(envConfig, Configs.ENV_PROPERTIES_PREFIX));
			configs.addConfiguration(envConfig);
			configs.addConfiguration(Configs.ConsumerDefaults);

			if (line.hasOption('f')) {
				final String file = line.getOptionValue('f');
				configs.addConfigurationFirst(ConfigUtils.newFileConfig(file));
			}

			if (line.hasOption('c')) {
				try {
					final MapSplitter split = Splitter.on(',').omitEmptyStrings().trimResults().withKeyValueSeparator('=');
					final Map<String, String> props = split.split(line.getOptionValue('c'));
					configs.addConfigurationFirst(new MapConfiguration(props));
				} catch (Exception e) {
					LOG.error("Failed to parse configs", e);
					printHelp();
					System.exit(1);
				}
			}

			LOG.info("Application Configuration:");
			ConfigUtils.printProperties(configs, (k, v) -> {
				LOG.info(String.format("\t%-60s = %s", k, v));
			});

			final PrometheusPuller pusher = new PrometheusPuller(configs);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						LOG.info("Shutting Down");
						pusher.close();
					} catch (IOException e) {
						LOG.error("Failed to shutdown currectly", e);
						System.err.println("Failed to shutdown currectly...");
						System.err.println(e);
					}
				}
			});

			LOG.info("Starting...");
			pusher.start();
			pusher.attach();
		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			printHelp();
			System.exit(1);
		}
	}

}
