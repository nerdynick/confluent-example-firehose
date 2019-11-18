package io.confluent.firehose;

import java.io.IOException;
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
import io.prometheus.client.exporter.PushGateway;

/**
 * Sample Implementation that exposes the Confluent Cloud Metrics Firehose to Prometheus 
 * using the Prometheus Push Gateway 
 * 
 * @author Nikoleta Verbeck
 *
 */
public class PrometheusPusher extends AbstractPrometheusFirehose {
	private static final Logger LOG = LoggerFactory.getLogger(PrometheusPusher.class);

	public static final String CONFIG_PROM_JOB_DEFAULT = PrometheusPusher.class.getSimpleName();

	public static final Options options = new Options();
	static {
		options.addOption("c", "config", true, "Comma Seperated key=value pairs of configs");
		options.addOption("f", "config-file", true, "Configs from a file");
		options.addOption("g", "gateway", true, "Prometheus PushGateway URL");
		options.addOption("j", "job", true, "Prometheus PushGateway Job Name");
		options.addOption("h", "help", false, "Print Help");
	}
	
	private PushGateway gateway;
	private String jobName;

	protected PrometheusPusher(Configuration config) {
		super(config);
		jobName = config.getString(Configs.CONFIG_PROM_GATEWAY_JOB, CONFIG_PROM_JOB_DEFAULT);
	}
	
	@Override
	public void start() {
		gateway = new PushGateway(config.getString(Configs.CONFIG_PROM_GATEWAY));
		super.start();
	}
	@Override
	protected void endOfSet() throws Exception{
		try {
			gateway.pushAdd(this.registry, jobName);
		} catch (IOException e) {
			LOG.error("Failed to push to gateway", e);
		}
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
			configs.addConfiguration(Configs.PrometheusGatewayDefaults);
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
			
			if(line.hasOption('g')) {
				configs.addProperty(Configs.CONFIG_PROM_GATEWAY, line.getOptionValue('g'));
			}
			
			if(line.hasOption('j')) {
				configs.addProperty(Configs.CONFIG_PROM_GATEWAY_JOB, line.getOptionValue('j'));
				
				//Optionally also set the Consumer Group ID if it hasn't been done so
				configs.addProperty(Configs.CONFIG_CONSUMER_GROUP_ID, configs.getString(Configs.CONFIG_CONSUMER_GROUP_ID, line.getOptionValue('j')));
			}

			LOG.info("PrometheusPusher Configuration:");
			ConfigUtils.printProperties(configs, (k, v) -> {
				LOG.info(String.format("\t%-60s = %s", k, v));
			});

			final PrometheusPusher pusher = new PrometheusPusher(configs);

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

			LOG.info("Starting Pusher");
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
