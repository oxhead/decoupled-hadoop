package org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.MinCostFlowModel.Arc;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.MinCostFlowModel.Network;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.flow.MinCostFlowModel.Node;

/**
 * http://www.igsystems.com/cs2/index.html
 * Must disable PRINT_ANS in MAKEFILE
 * @author oxhead
 *
 */
public class CS2Solver implements NetworkFlowSolver {

	private static final Log LOG = LogFactory.getLog(CS2Solver.class);

	private String solverPath;

	public CS2Solver(String solverPath) {
		this.solverPath = solverPath;
	}

	@Override
	public List<Solution> solve(Network networkModel) {
		LOG.fatal("[Solver] CS2");
		long start_time = System.currentTimeMillis();
		File inputFile = createInputFile(networkModel);
		LOG.fatal("CS2Solver: " + solverPath);
		List<String> resultList = executeCmd(solverPath, inputFile);
		long end_time = System.currentTimeMillis();
		LOG.fatal("[Solver] running Time = " + (end_time - start_time) / 1000.0);

		List<Solution> solutions = constructSolution(networkModel, resultList);

		return solutions;
	}

	private File createInputFile(Network net) {
		try {
			File inputFile = File.createTempFile("flow_", ".inp");
			//inputFile.deleteOnExit();
			LOG.fatal("[Solver] input file: " + inputFile.getPath());
			BufferedWriter writer = new BufferedWriter(new FileWriter(inputFile));

			List<Arc> arcList = net.arcList;

			writer.write("p min " + net.getNodeSize() + " " + net.getArcSize());
			writer.newLine();
			writer.write("n 1 " + net.getBalance());
			writer.newLine();
			writer.write("n 2 " + (-net.getBalance()));
			writer.newLine();
			for (Arc arc : arcList) {
				int index_head = net.getNodeIndex(arc.head);
				int index_tail = net.getNodeIndex(arc.tail);
				int cap = arc.maxCap;
				int cost = arc.cost;
				writer.append("a " + index_head + " " + index_tail + " 0 " + cap + " " + cost);
				writer.newLine();
			}
			writer.flush();
			writer.close();
			return inputFile;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	List<String> executeCmd(String solver, final File inputFile) {
		List<String> resultList = new LinkedList<String>();
		try {
			ProcessBuilder builder = new ProcessBuilder(solver);
			builder.redirectErrorStream(true);
			final Process process = builder.start();
			new Thread() {

				@Override
				public void run() {
					try {
						BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
						BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
						String content;
						while ((content = reader.readLine()) != null) {
							writer.write(content + "\n");
						}
						reader.close();
						writer.flush();
						writer.close();
					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}

			}.start();

			InputStreamReader isr = new InputStreamReader(process.getInputStream());
			BufferedReader br = new BufferedReader(isr);
			String line;

			while ((line = br.readLine()) != null) {
				if (line.startsWith("f")) {
					resultList.add(line);
				}
			}

			process.waitFor();

		} catch (Exception ex) {
			ex.printStackTrace();
			LOG.fatal("Unable to solve the min-cost flow network problem", ex);
		}

		return resultList;
	}

	private List<Solution> constructSolution(Network networkModel, List<String> resultList) {

		List<Solution> solutions = new LinkedList<Solution>();

		for (String result : resultList) {
			String[] tokents = result.split(" +");
			int headIndex = Integer.parseInt(tokents[1]);
			int tailIndex = Integer.parseInt(tokents[2]);
			int flow = Integer.parseInt(tokents[3]);
			Node head = networkModel.getNodeByIndex(headIndex);
			Node tail = networkModel.getNodeByIndex(tailIndex);
			Solution solution = new Solution(head, tail, flow);
			solutions.add(solution);
		}

		return solutions;
	}

}
