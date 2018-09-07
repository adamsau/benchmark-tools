package com.benchmark;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.sql.DataSource;
import javax.swing.*;
import java.awt.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
 * @benchmark connection pool when number of worker threads > fixed pool size 
 * and test the performance of offering connection to waiting threads
 * by the average time elapsed and 99.9 percentile.
*/
public class ConnectionPoolBenchmarkTest {
	public static final int TRIAL_COUNT = 3;
	public static final int THREAD_COUNT = 32;
	public static final int RUN_COUNT_PER_THREAD = 10000;

	//connection config
	public static final int INITIAL_CONNECTION_COUNT = 16;
	public static final int MAX_CONNECTION_COUNT = 16;
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
	public static final String CONNECT_URL = "jdbc://some_url";
	public static final String DB_TYPE = "mysql";

	public static void main(String[] args) throws Exception{
		List<DataSourceCommand> dataSourceCommands = dataSourceCommands();
		XYSeriesCollection xySeriesCollection = new XYSeriesCollection();

		for(DataSourceCommand dataSourceCommand: dataSourceCommands) {
			final DataSource dataSource = dataSourceCommand.dataSource();
			Object[] trialResults = new Object[(TRIAL_COUNT + 1) * THREAD_COUNT];

			//1 warm-up trial + N test trials
			System.out.println("testing datasource: " + dataSourceCommand.type());
			for (int i = 0; i < TRIAL_COUNT + 1; ++i) {
				if(i == 0) System.out.println("warmup running..");
				else System.out.println("trial " + i + " running..");
				ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

				CountDownLatch startLatch = new CountDownLatch(THREAD_COUNT);
				CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);

				for (int j = 0; j < THREAD_COUNT; ++j) {
					final int ii = i;
					final int jj = j;
					executorService.submit(() -> {
						try {
							List<Double> timeList = new ArrayList<>();

							startLatch.countDown();
							startLatch.await();

							//System.out.println("thread " + jj + ": GO!");

							for (int k = 0; k < RUN_COUNT_PER_THREAD; ++k) {
								long start = System.nanoTime();

								Connection con = dataSource.getConnection();

								new QuestionDao(con).exists("");

								con.close();

								long end = System.nanoTime();

								timeList.add(((double) (end - start)) / 1000000);
							}

							trialResults[ii * THREAD_COUNT + jj] = timeList;

							endLatch.countDown();
							endLatch.await();
						}
						catch (Exception e) {
							System.err.println(e);
						}
					});
				}
				executorService.shutdown();
				executorService.awaitTermination(1, TimeUnit.HOURS);
			}

			dataSourceCommand.destroy(dataSource);

			//output
			for (int i = 1; i < TRIAL_COUNT + 1; ++i) {
				List<Double> sortedTrialTimeList = new ArrayList<>();
				for (int j = 0; j < THREAD_COUNT; ++j) {
					List<Double> timeList = (List<Double>) trialResults[i * THREAD_COUNT + j];
					sortedTrialTimeList.addAll(timeList);
				}
				sortedTrialTimeList.sort(Comparator.naturalOrder());

				int ninePercentile = (int) (THREAD_COUNT * RUN_COUNT_PER_THREAD * 0.999);
				System.out.println(String.format("trial %d: average: %.3f min: %.3f max: %.3f 99.9-percentile: %.3f total_elapsed_time: %.3f",
						i,
						sortedTrialTimeList.stream().reduce(Double::sum).get() / sortedTrialTimeList.size(),
						sortedTrialTimeList.get(0),
						sortedTrialTimeList.get(sortedTrialTimeList.size() - 1),
						sortedTrialTimeList.get(ninePercentile),
						sortedTrialTimeList.stream().reduce(Double::sum).get()));

				XYSeries xySeries = new XYSeries(dataSourceCommand.type() + "-t" + i);
				for(int cnt = 0; cnt < ninePercentile; ++cnt) xySeries.add(cnt, sortedTrialTimeList.get(cnt));
				xySeriesCollection.addSeries(xySeries);
			}

			System.out.println();
		}

		JFreeChart chart = ChartFactory.createXYLineChart("", "request", "time elapsed", xySeriesCollection);

		JFrame jFrame = new JFrame();
		jFrame.add(new ChartPanel(chart), BorderLayout.CENTER);
		jFrame.setSize(1920,1080);
		jFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		jFrame.setLocationRelativeTo(null);

		jFrame.setVisible(true);
	}

	private interface DataSourceCommand {
		String type();
		DataSource dataSource();
		void destroy(DataSource dataSource);
	}

	public static List<DataSourceCommand> dataSourceCommands() {
		List<DataSourceCommand> dataSourceCommands = new ArrayList<>();
		dataSourceCommands.add(new DataSourceCommand() {
			@Override
			public String type() {
				return "hikaricp";
			}

			@Override
			public DataSource dataSource() {
				HikariConfig hikariConfig = new HikariConfig();
				hikariConfig.setJdbcUrl(CONNECT_URL);
				hikariConfig.setDriverClassName(DRIVER_NAME);
				hikariConfig.setUsername(USERNAME);
				hikariConfig.setPassword(PASSWORD);
				hikariConfig.setMaximumPoolSize(MAX_CONNECTION_COUNT);


				return new HikariDataSource(hikariConfig);
			}

			@Override
			public void destroy(DataSource dataSource) {
				((HikariDataSource) dataSource).close();
			}
		});
		dataSourceCommands.add(new DataSourceCommand() {
			@Override
			public String type() {
				return "tomcat-jdbc-pool";
			}

			@Override
			public DataSource dataSource() {
				PoolProperties poolProperties = new PoolProperties();
				poolProperties.setUrl(CONNECT_URL);
				poolProperties.setDriverClassName(DRIVER_NAME);
				poolProperties.setUsername(USERNAME);
				poolProperties.setPassword(PASSWORD);
				poolProperties.setMaxIdle(INITIAL_CONNECTION_COUNT);
				poolProperties.setMaxActive(MAX_CONNECTION_COUNT);
				poolProperties.setInitialSize(INITIAL_CONNECTION_COUNT);
				poolProperties.setFairQueue(true);


				return new org.apache.tomcat.jdbc.pool.DataSource(poolProperties);
			}

			@Override
			public void destroy(DataSource dataSource) {
				((org.apache.tomcat.jdbc.pool.DataSource) dataSource).close();
			}
		});

		return dataSourceCommands;
	}
}
