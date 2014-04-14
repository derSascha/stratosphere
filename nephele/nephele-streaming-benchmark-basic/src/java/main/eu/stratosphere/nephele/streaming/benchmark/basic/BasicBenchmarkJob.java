package eu.stratosphere.nephele.streaming.benchmark.basic;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.ConstraintUtil;

/**
 * @author Sascha Wolke
 */
public class BasicBenchmarkJob {
	/**
	 * Arguments: [jarFile] [jobManagerAdress] [forwardingTaskCount] [delayBetweenPackages in ns] [packageCount] [packageSize]
	 */
	public static void main(String[] args) throws Exception {
		final JobGraph jobGraph = new JobGraph("Basic streaming benchmark job");
		
		boolean recompileWithMaven = true;
		String jarFile = "target/nephele-streaming-benchmark-basic-streaming-git.jar";
		String jobManagerAdress = "127.0.0.1";
		int forwardingTaskCount = 3;
		long delayBetweenPackagesNS = 10*1000; // in nano seconds
		int packageCount = 10000;
		int packageSize = 26*1024;
		
		// parse cmd arguments
		if (args.length > 0) {
			recompileWithMaven = false;
			jarFile = args[0];
			if (args.length > 1)
				jobManagerAdress = args[1];
			if (args.length > 2)
				delayBetweenPackagesNS = Long.parseLong(args[2]);
			if (args.length > 3)
				packageCount = Integer.parseInt(args[3]);
			if (args.length > 4)
				packageSize = Integer.parseInt(args[4]);
		}
		
		final JobInputVertex packageGenerator = new JobInputVertex("PackageGenerator", jobGraph);
		packageGenerator.setInputClass(PackageGenerator.class);
		packageGenerator.setNumberOfSubtasks(1);
		packageGenerator.getConfiguration().setInteger(PackageGenerator.USER_TASK_TIMING_COUNT_CONFIG_KEY, forwardingTaskCount + 2);
		packageGenerator.getConfiguration().setLong(PackageGenerator.DELAY_BETWEEN_PACKEGES_NS_CONFIG_KEY, delayBetweenPackagesNS);
		packageGenerator.getConfiguration().setInteger(PackageGenerator.PACKAGE_COUNT_CONFIG_KEY, packageCount);
		packageGenerator.getConfiguration().setInteger(PackageGenerator.PACKAGE_SIZE_CONFIG_KEY, packageSize);

		final JobTaskVertex forwardingTasks[] = new JobTaskVertex[forwardingTaskCount];
		forwardingTasks[0] = new JobTaskVertex("Forwarder-0", jobGraph);
		forwardingTasks[0].setTaskClass(FowardTask.class);
		forwardingTasks[0].setNumberOfSubtasks(1);
		packageGenerator.connectTo(forwardingTasks[0], ChannelType.NETWORK);
		for (int i = 1; i < forwardingTaskCount; i++) {
			forwardingTasks[i] = new JobTaskVertex("Forwarder-" + i, jobGraph);
			forwardingTasks[i].setTaskClass(FowardTask.class);
			forwardingTasks[i].setNumberOfSubtasks(1);
			forwardingTasks[i - 1].connectTo(forwardingTasks[i], ChannelType.NETWORK);
		}
		
		final JobFileOutputVertex packageCollector = new JobFileOutputVertex("PackageCollector", jobGraph);
		packageCollector.setFileOutputClass(PackageCollector.class);
		packageCollector.setFilePath(new Path("file:///tmp/basic_benchmark_timings." + jobGraph.getJobID() + ".json"));
		packageCollector.setNumberOfSubtasks(1);
		packageCollector.getConfiguration().setInteger(PackageGenerator.USER_TASK_TIMING_COUNT_CONFIG_KEY, forwardingTaskCount + 2);
		forwardingTasks[forwardingTasks.length - 1].connectTo(packageCollector, ChannelType.NETWORK);
		
		ConstraintUtil.defineAllLatencyConstraintsBetween(forwardingTasks[0],
				forwardingTasks[forwardingTasks.length - 1], forwardingTaskCount*2);
				
		if (recompileWithMaven) {
			System.err.println("Running maven...");
			Process p = Runtime.getRuntime().exec("mvn package -DskipTests=true -Dmaven.javadoc.skip=true");
			p.waitFor();
		}
		
		jobGraph.addJar(new Path(jarFile));
				
		Configuration conf = new Configuration();
		conf.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, jobManagerAdress);
		conf.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		System.err.println("Launching job...");
		final JobClient jobClient = new JobClient(jobGraph, conf);
		jobClient.submitJobAndWait();
	}

}
