package storm.metrics;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

//import com.github.staslev.storm.metrics.MetricReporter;
//import com.github.staslev.storm.metrics.MetricReporterConfig;
//import com.github.staslev.storm.metrics.yammer.SimpleGraphiteStormMetricProcessor;
//import com.github.staslev.storm.metrics.yammer.StormYammerMetricsAdapter;
//import com.github.staslev.storm.metrics.yammer.YammerFacadeMetric;
//import com.yammer.metrics.core.MetricsRegistry;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import java.util.Arrays;
import java.util.UUID;
import java.util.Map;
import java.util.concurrent.TimeUnit;



public class KafkaMetricsTopology {
         public static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsTopology.class);

         public static class PrinterBolt extends BaseBasicBolt {

                  @Override
                   public void declareOutputFields(OutputFieldsDeclarer declarer) {
                   }

                  @Override
                  public void execute(Tuple tuple, BasicOutputCollector collector) {
                                   LOG.info(tuple.toString());
                  }
         }

         public static class BoltThatAlsoReportsToGraphite extends BaseBasicBolt {

                  private static final String GRAPHITE_HOST = "10.100.70.250";
                  private static final int CARBON_AGGREGATOR_LINE_RECEIVER_PORT = 2023;
                         // The following value must match carbon-cache's storage-schemas.conf!
                  private static final int GRAPHITE_REPORT_INTERVAL_IN_SECONDS = 10;
                  private static final String GRAPHITE_METRICS_NAMESPACE_PREFIX = "production.apps.graphitedemo";
                  private static final Pattern hostnamePattern = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]*(\\.([a-zA-Z0-9][a-zA-Z0-9-]*))*$");
                  private transient Meter tuplesReceived;

                  @Override
                  public void prepare(Map stormConf, TopologyContext context) {
                         initializeMetricReporting();
                  }

                  private void initializeMetricReporting() {
                           final MetricRegistry registry = new MetricRegistry();
                           final Graphite graphite = new Graphite(new InetSocketAddress(GRAPHITE_HOST, CARBON_AGGREGATOR_LINE_RECEIVER_PORT));
                           final GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                                                             .prefixedWith(metricsPath())
                                                             .convertRatesTo(TimeUnit.SECONDS)
                                                             .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                             .filter(MetricFilter.ALL)
                                                             .build(graphite);
                          reporter.start(GRAPHITE_REPORT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
                          tuplesReceived = registry.meter(MetricRegistry.name("tuples", "received"));
                  }

                  private String metricsPath() {
                          final String myHostname = extractHostnameFromFQHN(detectHostname());
                          return GRAPHITE_METRICS_NAMESPACE_PREFIX + "." + myHostname;
                  }

                 @Override
                 public void execute(Tuple tuple, BasicOutputCollector collector) {
                               tuplesReceived.mark();

                 // FYI: We do not need to explicitly ack() the tuple because we are extending
                 // BaseBasicBolt, which will automatically take care of that.
                 }


                 private String detectHostname() {
                              String hostname = "hostname-could-not-be-detected";
                              try {
                                     hostname = InetAddress.getLocalHost().getHostName();
                              }
                              catch (UnknownHostException e) {
                                         LOG.error("Could not determine hostname");
                              }
                              return hostname;
                 }

                 private static String extractHostnameFromFQHN(String fqhn) {
                      if (hostnamePattern.matcher(fqhn).matches()) {
                           if (fqhn.contains(".")) {
                                    return fqhn.split("\\.")[0];
                                }
                           else {
                                    return fqhn;
                                }
                      }
                      else {
                              // We want to return the input as-is
                                 // when it is not a valid hostname/FQHN.
                                    return fqhn;
                           }
                      }

                 @Override
                 public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

                 }
         }


         private static BrokerHosts brokerHosts = null;
         public KafkaMetricsTopology(String kafkaZookeeper) {
                    brokerHosts = new ZkHosts(kafkaZookeeper);
         }


         public static void main(String[] args) throws Exception {

                String brokerIp        = "10.100.71.33:9092" ;
                String kafkaZk         = "10.100.71.33:2181" ;
                String topicName       = "pipe-test-3" ;
                String nimbushost      = "10.100.71.33" ;
                String stormZK         = "10.100.71.33" ;

                KafkaMetricsTopology kafkaMetricsTopology = new KafkaMetricsTopology(kafkaZk);
                Config config = new Config();
                config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
                config.put("kafka.spout.buffer.size.max", 10);

                config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,             8);
                config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,            32);
                config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
                config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    16384);


//                config.put(YammerFacadeMetric.FACADE_METRIC_TIME_BUCKET_IN_SEC, 10);
//                config.put(MetricReporter.METRICS_HOST, "10.100.70.29");
//                config.put(MetricReporter.METRICS_PORT, "2003");
//                config.put(YammerFacadeMetric.FACADE_METRIC_TIME_BUCKET_IN_SEC, 10);
//                config.registerMetricsConsumer(MetricReporter.class,
//                                               new MetricReporterConfig("(.*execute.*|.*latency.*|.*capacity.*)",
//                                                                        SimpleGraphiteStormMetricProcessor.class.getCanonicalName()),
//                                               1);




                SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topicName, "/"+topicName, UUID.randomUUID().toString());
                spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                spoutConfig.forceFromStart = true;

                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("words", new KafkaSpout(spoutConfig), 6);
                builder.setBolt("print", new PrinterBolt()).shuffleGrouping("words");
                builder.setBolt("Report", new BoltThatAlsoReportsToGraphite());

                if (args != null && args.length > 1) {
                        String name = args[1];
                        config.setNumWorkers(2);
                        config.setMaxTaskParallelism(5);
                        config.put(Config.NIMBUS_HOST, nimbushost);
                        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
                        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
                        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(stormZK));
                        StormSubmitter.submitTopology(name, config, builder.createTopology());
                } else {
                        config.setNumWorkers(6);
                        config.setMaxTaskParallelism(10);
                        LocalCluster cluster = new LocalCluster();
                        cluster.submitTopology("kafka", config, builder.createTopology());
                }
         }
}
