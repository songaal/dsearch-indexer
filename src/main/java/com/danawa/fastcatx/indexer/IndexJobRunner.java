package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.danawa.fastcatx.indexer.ingester.*;
import com.danawa.fastcatx.indexer.model.JdbcMetaData;
//import com.danawa.fastcatx.indexer.preProcess.VmFirstMakeDatePreProcess;
import com.danawa.fastcatx.indexer.preProcess.PreProcess;
import com.github.fracpete.processoutput4j.output.CollectingProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class IndexJobRunner implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(IndexJobRunner.class);
	public enum STATUS { READY, RUNNING, SUCCESS, ERROR, STOP }
	private Job job;
	private IndexService service;
	private Ingester ingester;

	private final RestTemplate restTemplate = new RestTemplate(Utils.getRequestFactory());
	private final Gson gson = new Gson();

	private boolean autoDynamic;
	private String autoDynamicIndex;
	private List<String> autoDynamicQueueNames;
	private String autoDynamicCheckUrl;
	private String autoDynamicQueueIndexUrl;
	private int autoDynamicQueueIndexConsumeCount = 1;
	private boolean preProcess;

	boolean enableRemoteCmd;
	String remoteCmdUrl;
	private boolean isCreatedIndex;

	public IndexJobRunner(Job job) {
		this.job = job;
		job.setStatus(STATUS.READY.name());
		job.setStartTime(System.currentTimeMillis() / 1000);
		service = null;
		ingester = null;
	}

	public String getDirectoryAllFiles(String path) {
		StringBuffer sb = new StringBuffer();

		try {
			if(Files.isDirectory(Paths.get(path))){
				logger.info("{} is directory !", path);
				Files.walk(Paths.get(path))
				.filter(Files::isRegularFile)
				.forEach(item -> {
					if(sb.length() == 0){
						sb.append(item.toString());
					}else{
						sb.append("," + item.toString());
					}
				});
			}else{
				logger.info("{} is regularFile !", path);
				sb.append(path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return sb.toString();
	}

	@Override
	public void run() {
		try {
			job.setStatus(STATUS.RUNNING.name());
			Map<String, Object> payload = job.getRequest();
			logger.debug("{}", gson.toJson(payload));

			// ????????? ??????????????? ??????
			preProcess = (Boolean) payload.getOrDefault("preProcess", false);
			// ????????? ????????? ??????
			if (preProcess) {
				// ???????????? ???????????? ON/OFF ??????
				autoDynamic = (Boolean) payload.getOrDefault("autoDynamic",false);
				if(autoDynamic) {
					disableAutoDynamic(payload);
				}
				PreProcess process = new PreProcess.EmptyPreProcess();
				process.starter(job);
				return;
			}

			logger.info("Started Indexing Job Runner");
			// ??????
			// ES ?????????
			String host = (String) payload.get("host");
			// ES ??????
			Integer port = (Integer) payload.get("port");
			String esUsername = null; // ES ??????
			String esPassword = null; // ES ????????????
			if (payload.get("esUsername") != null && payload.get("esPassword") != null) {
				esUsername = (String)payload.get("esUsername");
				esPassword = (String)payload.get("esPassword");
			}

			// http, https
			String scheme = (String) payload.get("scheme");
			// ????????????.
			String index = (String) payload.get("index");
			// ????????????: ndjson, csv, jdbc ???..
			String type = (String) payload.get("type");
			// index??? ???????????? ???????????? ???????????? ??????. indexTemplate ??? ????????? ????????? ????????? ?????? ???????????? ??????.
			Boolean reset = (Boolean) payload.getOrDefault("reset", true);
			// ?????? ????????? ??????. ?????????????????? ???????????? ??????. ???) com.danawa.fastcatx.filter.MockFilter
			String filterClassName = (String) payload.get("filterClass");
			// ES bulk API ????????? ????????????.
			Integer bulkSize = (Integer) payload.get("bulkSize");
			Integer threadSize = (Integer) payload.getOrDefault("threadSize", 1);
			String pipeLine = (String) payload.getOrDefault("pipeLine","");

			Map<String, Object> indexSettings;
			try {
				indexSettings = (Map<String, Object>) payload.get("_indexingSettings");
				if (indexSettings == null) {
					throw new ClassCastException();
				}
			} catch (ClassCastException e) {
				indexSettings = new HashMap<>();
			}
			logger.debug("index: {}", index);
			/**
			 * file?????? ???????????? ??????
			 */
			//?????? ??????.
			String path = (String) payload.get("path");
			// ?????? ?????????. utf-8, cp949 ???..
			String encoding = (String) payload.get("encoding");
			// ?????????????????? ????????? ????????? ???????????? ????????? ??????.
			Integer limitSize = (Integer) payload.getOrDefault("limitSize", 0);

			// ???????????? ???????????? on/off
			autoDynamic = (Boolean) payload.getOrDefault("autoDynamic",false);

			if (autoDynamic) {
				//                    ???????????? ???????????? ?????? ????????????
				disableAutoDynamic(payload);
			}

			boolean dryRun = (Boolean) payload.getOrDefault("dryRun",false); // rsync ?????? ??????
			if (dryRun) {
				int r = (int) Math.abs((Math.random() * 999999) % 120) * 1000;
				Thread.sleep(r);
				job.setStatus(STATUS.SUCCESS.name());
				logger.info("[DRY_RUN] index: {} procedure Type {}. Index Success", index, type);
				return;
			}

			if (type.equals("ndjson")) {
				ingester = new NDJsonIngester(path, encoding, 1000, limitSize);
			} else if (type.equals("csv")) {
				ingester = new CSVIngester(path, encoding, 1000, limitSize);
			} else if (type.equals("file")) {
				String headerText = (String) payload.get("headerText");
				String delimiter = (String) payload.get("delimiter");
				ingester = new DelimiterFileIngester(path, encoding, 1000, limitSize, headerText,delimiter);
			} else if(type.equals("konan")){
				ingester = new KonanIngester(path, encoding, 1000, limitSize);
			}else if (type.equals("jdbc")) {
				String dataSQL = (String) payload.get("dataSQL");
				Integer fetchSize = (Integer) payload.get("fetchSize");
				Integer maxRows = (Integer) payload.getOrDefault("maxRows", 0);
				Boolean useBlobFile = (Boolean) payload.getOrDefault("useBlobFile", false);
				if (payload.get("_jdbc") != null) {
					Map<String, Object> jdbcMap = (Map<String, Object>) payload.get("_jdbc");

					int sqlCount = 2;
					ArrayList<String> sqlList = new ArrayList<String>();

					String driverClassName = (String) jdbcMap.get("driverClassName");
					String url = (String) jdbcMap.get("url");
					String user = (String) jdbcMap.get("user");
					String password = (String) jdbcMap.get("password");

					sqlList.add(dataSQL);
					//dataSQL2,3.... ?????? ??????
					while ( payload.get("dataSQL" + String.valueOf(sqlCount)) != null ) {
						sqlList.add((String) payload.get("dataSQL" + String.valueOf(sqlCount)));
						sqlCount++;
					}

					ingester = new JDBCIngester(driverClassName, url, user, password, bulkSize, fetchSize, maxRows, useBlobFile, sqlList);
				} else {
					throw new IllegalArgumentException("jdbc argument");
				}
			} else if (type.equals("multipleJdbc")) {
				String dataSQL = (String) payload.get("dataSQL"); // ??? ????????????
				String dataSubSQL = (String) payload.get("dataSubSQL"); // ?????? ?????? ??????

				Integer fetchSize = (Integer) payload.get("fetchSize");
				Integer maxRows = (Integer) payload.getOrDefault("maxRows", 0);
				Boolean useBlobFile = (Boolean) payload.getOrDefault("useBlobFile", false);

				String subSqlwhereclauseData = (String) payload.getOrDefault("subSqlwhereclauseData", "");

				if (payload.get("_jdbc") != null) {
					Map<String, Object> jdbcMap = (Map<String, Object>) payload.get("_jdbc");
					Map<String, JdbcMetaData> jdbcMetaDataMap = new HashMap<>();
					Map<String, ArrayList<String>> sqlQueryMap = new HashMap<>();

					// ?????? Query ???  JDBC ??????????????? ?????? ??????
					int sqlCount = 2;
					ArrayList<String> sqlList = new ArrayList<String>();

					String driverClassName = (String) jdbcMap.get("driverClassName");
					String url = (String) jdbcMap.get("url");
					String user = (String) jdbcMap.get("user");
					String password = (String) jdbcMap.get("password");
					sqlList.add(dataSQL);
					//dataSQL2,3.... ?????? ??????
					while ( payload.get("dataSQL" + sqlCount) != null ) {
						sqlList.add((String) payload.get("dataSQL" + sqlCount));
						sqlCount++;
					}

					JdbcMetaData  mainMetaData = new JdbcMetaData();
					mainMetaData.setDriverClassName(driverClassName);
					mainMetaData.setUrl(url);
					mainMetaData.setUser(user);
					mainMetaData.setPassword(password);

					jdbcMetaDataMap.put("mainJDBC", mainMetaData);

					logger.info("mainSqlList COUNT : {}", sqlList.size());

					sqlQueryMap.put("mainSqlList", sqlList);


					if ((boolean) payload.get("isMultipleJDBC")) { // ?????? ??????
						ArrayList<String> subSqlList = new ArrayList<String>();
						// ?????? Query ???  JDBC ??????????????? ?????? ??????
						driverClassName = (String) payload.get("subDriverClassName");
						url =  (String) payload.get("subUrl");
						user =(String) payload.get("subUser");
						password = (String) payload.get("subPassword");

						subSqlList.add(dataSubSQL);

						logger.info("subSqlList COUNT : {}", subSqlList.size());
						sqlQueryMap.put("subSqlList", subSqlList);

						JdbcMetaData  subMetaData = new JdbcMetaData();
						subMetaData.setDriverClassName(driverClassName);
						subMetaData.setUrl(url);
						subMetaData.setUser(user);
						subMetaData.setPassword(password);

						jdbcMetaDataMap.put("subJDBC", subMetaData);
					}

					ingester = new MultipleJDBCIngester(jdbcMetaDataMap, bulkSize, fetchSize, maxRows, useBlobFile,sqlQueryMap, subSqlwhereclauseData);
				} else {
					throw new IllegalArgumentException("jdbc argument");
				}
			}
			else if (type.equals("multipleDumpFile")) {
				// ?????? ??????
				new MultipleDumpFile().index(job, host, port, esUsername, esPassword, scheme, index, reset, filterClassName, bulkSize, threadSize, pipeLine, indexSettings, payload);
				return;
			} else if (type.equals("procedure")) {

				//???????????? ????????? ????????? ??????
				String driverClassName = (String) payload.get("driverClassName");
				String url = (String) payload.get("url");
				String user = (String) payload.get("user");
				String password = (String) payload.get("password");
				String procedureName = (String) payload.getOrDefault("procedureName","PRSEARCHPRODUCT"); //PRSEARCHPRODUCT
				Integer groupSeq = null;
				if (payload.get("groupSeq") != null) {
					try {
						groupSeq = (Integer) payload.get("groupSeq");
					} catch (Exception e) {
						groupSeq = Integer.parseInt((String) payload.get("groupSeq"));
					}
				}
				String dumpFormat = (String) payload.get("dumpFormat"); //ndjson, konan
				String rsyncPath = (String) payload.get("rsyncPath"); //rsync - Full Path
				String rsyncIp = (String) payload.get("rsyncIp"); // rsync IP
				String bwlimit = (String) payload.getOrDefault("bwlimit","0"); // rsync ???????????? - 1024 = 1m/s
				boolean procedureSkip  = (Boolean) payload.getOrDefault("procedureSkip",false); // ???????????? ?????? ??????
				boolean rsyncSkip = (Boolean) payload.getOrDefault("rsyncSkip",false); // rsync ?????? ??????

				//????????????
				CallProcedure procedure = new CallProcedure(driverClassName, url, user, password, procedureName,groupSeq,path);
				//RSNYC
				RsyncCopy rsyncCopy = new RsyncCopy(rsyncIp,rsyncPath,path,bwlimit,groupSeq);

				boolean execProdure = false;
				boolean rsyncStarted = false;
				//???????????? ??????
				String dumpFileName = "prodExt_"+groupSeq;

				//SKIP ????????? ?????? ???????????? ??????
				if(procedureSkip == false) {
					execProdure = procedure.callSearchProcedure();
				}
				//                logger.info("execProdure : {}",execProdure);

				//???????????? ?????? True, R ??????X or ???????????? ?????? and rsync ??????X
				if((execProdure && rsyncSkip == false) || (procedureSkip && rsyncSkip == false)) {
					rsyncCopy.start();
					Thread.sleep(3000);
					rsyncStarted = rsyncCopy.copyAsync();
				}
				logger.info("rsyncStarted : {}" , rsyncStarted );
				int fileCount = 0;
				if(rsyncStarted || rsyncSkip) {

					if(rsyncSkip) {
						logger.info("rsyncSkip : {}" , rsyncSkip);

						long count = 0;
						if(Files.isDirectory(Paths.get(path))){
							count = Files.walk(Paths.get(path)).filter(Files::isRegularFile).count();
						}else if(Files.isRegularFile(Paths.get(path))){
							count = 1;
						}

						if(count == 0){
							throw new FileNotFoundException("????????? ?????? ??? ????????????. (filepath: " + path + "/)");
						}
					} else {
						//????????? ????????? 1????????? ??????
						while (!Utils.checkFile(path, dumpFileName)) {
							if (fileCount == 10) break;
							Thread.sleep(1000);
							fileCount++;
							logger.info("{} ?????? ?????? count: {} / 10", dumpFileName, fileCount);
						}

						if (fileCount == 10) {
							throw new FileNotFoundException("rsync ??? ????????? ?????? ??? ????????????. (filepath: " + path + "/" + dumpFileName + ")");
						}
					}
					//GroupSeq??? ????????? ????????????????????? ??????+?????????????????? ???????????? ??????
					//                    path += "/"+dumpFileName;
					logger.info("file Path - Name  : {} - {}", path, dumpFileName);
					ingester = new ProcedureIngester(path, dumpFormat, encoding, 1000, limitSize);
				}
			}
			else if (type.equals("procedure-link")) {

				//???????????? ????????? ????????? ??????
				String driverClassName = (String) payload.get("driverClassName");
				String url = (String) payload.get("url");
				String user = (String) payload.get("user");
				String password = (String) payload.get("password");
				String procedureName = (String) payload.getOrDefault("procedureName","PRSEARCHPRODUCT"); //PRSEARCHPRODUCT
				String groupSeqs = (String) payload.get("groupSeqs");
				String dumpFormat = (String) payload.get("dumpFormat"); //ndjson, konan
				String rsyncPath = (String) payload.get("rsyncPath"); //rsync - Full Path
				String rsyncIp = (String) payload.get("rsyncIp"); // rsync IP
				String bwlimit = (String) payload.getOrDefault("bwlimit","0"); // rsync ???????????? - 1024 = 1m/s
				boolean procedureSkip  = (Boolean) payload.getOrDefault("procedureSkip",false); // ???????????? ?????? ??????
				boolean rsyncSkip = (Boolean) payload.getOrDefault("rsyncSkip",false); // rsync ?????? ??????
				String procedureThreads = (String) payload.getOrDefault("procedureThreads","4"); // rsync ?????? ??????
				//            ?????? ?????? ?????? ?????? (???????????? ????????????)
				enableRemoteCmd = (boolean) payload.getOrDefault("enableRemoteCmd",false);
				//            ?????? ?????? URL (???????????? ????????????)
				remoteCmdUrl = (String) payload.getOrDefault("remoteCmdUrl","");

				// ?????????es ???????????? ???????????? ??????
				boolean enableOfficeIndexingJob = (Boolean) payload.getOrDefault("enableOfficeIndexingJob", false);
				String officeFullIndexUrl = (String) payload.getOrDefault("officeFullIndexUrl","");

				String[] groupSeqLists = groupSeqs.split(",");

				// 1. groupSeqLists ??? ?????? ???????????? ??????
				// 2. ??????
				// 3. ??? ????????? rsync
				StringBuffer sb = new StringBuffer();
				Map<String, Boolean> procedureMap = new HashMap<>();

				if (enableRemoteCmd) {
					remoteCmd("CLOSE", 10);
				}

				if(procedureSkip == false) {
					logger.info("Call Procedure");
					ExecutorService threadPool = Executors.newFixedThreadPool(Integer.parseInt(procedureThreads));

					List threadsResults = new ArrayList<Future<Object>>();
					for(String groupSeq : groupSeqLists){
						Integer groupSeqNumber = Integer.parseInt(groupSeq);
						Callable callable = new Callable() {
							@Override
							public Object call() throws Exception {
								String path = (String) payload.get("path");
								logger.info("driverClassName: {}, url: {}, user: {}, password: {}, procedureName: {}, groupSeqNumber: {}, path: {}", driverClassName, url, user, password, procedureName, groupSeqNumber, path);
								CallProcedure procedure = new CallProcedure(driverClassName, url, user, password, procedureName, groupSeqNumber, path, true);
								Map<String, Object> result = new HashMap<>();
								result.put("groupSeq", groupSeq);
								result.put("result", procedure.callSearchProcedure());
								return result;
							}
						};

						Future<Object> future = threadPool.submit(callable);
						threadsResults.add(future);
					}

					for (Object f : threadsResults) {
						Future<Object> future = (Future<Object>) f;
						Map<String, Object> execProdure = (Map<String, Object>) future.get();
						procedureMap.put((String) execProdure.get("groupSeq"), (Boolean) execProdure.get("result"));
						logger.info("{} execProdure: {}", (String) execProdure.get("groupSeq"), (Boolean) execProdure.get("result"));
					}

					threadPool.shutdown();
				}

//                ???????????? -> multiThread
//                rsync ->  singleThread -> 1??? ???
				if (enableRemoteCmd) {
					remoteCmd("INDEX", 10);
				}

				if (enableOfficeIndexingJob) {
					officeLinkIndexing(officeFullIndexUrl, groupSeqs);
				}

				if(rsyncSkip == false){
					ExecutorService threadPool = Executors.newFixedThreadPool(Integer.parseInt(procedureThreads));
					List threadsResults2 = new ArrayList<Future<Object>>();

					// ?????? ??????
					logger.info("path={}", path);
					File file = new File(path);
					if(!file.exists()){
						boolean result = file.mkdir();
						if(result){
							logger.info("{} mkdir!!", path);
						}else{
							logger.info("{} mkdir failed..", path);
						}
					}

					for(String groupSeq : groupSeqLists){
						logger.info("groupSeq : {}", groupSeq);
						Integer groupSeqNumber = Integer.parseInt(groupSeq);

						Callable callable = new Callable() {
							@Override
							public Object call() throws Exception {
								String path = (String) payload.get("path");
								String dumpFileName = "linkExt_" + groupSeq;

								//???????????? ??????
								File file = new File(path + "/" + dumpFileName);
								logger.info("fila path: {}", path + "/" + dumpFileName);

								if (file.exists()) {
									logger.info("?????? ?????? ?????? : {}", file);
									file.delete();
								}else{
									logger.info("?????? ????????? ????????? ??????: {}", file);
								}

								boolean execProdure = false;
								boolean rsyncStarted = false;
								//SKIP ????????? ?????? ???????????? ??????
								if(procedureSkip == false) {
									execProdure = procedureMap.get(groupSeq);
								}
								//                                logger.info("execProdure : {}",execProdure);
								RSync rsync = new RSync()
										.source(rsyncIp+"::" + rsyncPath+"/linkExt_"+groupSeqNumber)
										.destination(path)
										.recursive(true)
										.archive(true)
										.compress(true)
										.bwlimit(bwlimit)
										.inplace(true);

								//???????????? ?????? True, R ??????X or ???????????? ?????? and rsync ??????X
								if((execProdure && rsyncSkip == false) || (procedureSkip && rsyncSkip == false)) {
									CollectingProcessOutput output = rsync.execute();
									if( output.getExitCode() > 0){
										logger.error("{}", output.getStdErr());
									}
								}
								logger.info("rsyncStarted : {}" , rsyncStarted );

								if(rsyncStarted || rsyncSkip) {
									if(rsyncSkip) {
										logger.info("rsyncSkip : {}" , rsyncSkip);
									}
								}
								String filepath = path + "/" + dumpFileName;

								if(!rsyncSkip) {
									//????????? ????????? 1????????? ??????
									int fileCount = 0;
									while(!Utils.checkFile(path, dumpFileName)){
										if(fileCount == 10) break; // ???????????? ??????
										Thread.sleep(1000);
										fileCount++;
										logger.info("{} ?????? ?????? count: {} / 10", dumpFileName, fileCount);
									}

									if(fileCount == 10){
										throw new FileNotFoundException("rsync ??? ????????? ?????? ??? ????????????. (filepath: "+path + "/" + dumpFileName + ")");
									}
								}
								return filepath;
							}
						};
						Future<Object> future = threadPool.submit(callable);
						threadsResults2.add(future);
					}

					for (Object f : threadsResults2) {
						Future<Object> future = (Future<Object>) f;
						String filepath = (String) future.get();
						logger.info("filepath: {}", filepath);
						sb.append(filepath + ",");
					}

					threadPool.shutdown();
				}
				else{
					sb.append(path);
					//                    for(String groupSeq : groupSeqLists){
					//                        String filepath = (String) payload.get("path");
					//                        String dumpFileName = "linkExt_"+groupSeq;
					//                        sb.append(filepath + "/" + dumpFileName + ",");
					//                    }
				}

				if(sb.length() > 0 && sb.charAt(sb.length()-1) == ','){
					sb.deleteCharAt(sb.length()-1);
				}

				ingester = new ProcedureLinkIngester(sb.toString(), dumpFormat, encoding, 1000, limitSize);
			}

			Ingester finalIngester = ingester;
			Filter filter = (Filter) Utils.newInstance(filterClassName);

			if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
				logger.info("[STOP SIGNAL] type: procedure");
				throw new StopSignalException();
			}
			//            service = new IndexService(host, port, scheme);
			service = new IndexService(host, port, scheme, esUsername, esPassword);
			// ???????????? ??????????????? 0????????? ???????????????.
			if (reset) {
				isCreatedIndex = service.retryUntilCreatingIndex(index, indexSettings, 30);

				if (isCreatedIndex) {
					logger.info("Index Created Success!! index: {}", index);
				} else {
					logger.info("Index ReCreated Fail... index: {}", index);
				}
			}

			if(type.equals("reindex")){
				service.reindex(payload, index, job);
			} else {
				// reindex??? ?????? ?????? ?????? ????????? ??????
				if (threadSize > 1) {
					service.indexParallel(finalIngester, index, bulkSize, filter, threadSize, job, pipeLine);
				} else {
					service.index(finalIngester, index, bulkSize, filter, job, pipeLine);
				}
			}

			job.setStatus(STATUS.SUCCESS.name());
		} catch (StopSignalException e) {
			job.setStatus(STATUS.STOP.name());
		} catch (FileNotFoundException e){
			job.setStatus(STATUS.STOP.name());
			job.setError(e.getMessage());
			logger.error("error .... ", e);
		} catch (Exception e) {
			job.setStatus(STATUS.ERROR.name());
			job.setError(e.getMessage());
			logger.error("error .... ", e);
		} finally {
			job.setEndTime(System.currentTimeMillis() / 1000);
			if (autoDynamic) {
				new Thread(() -> {
					int r = 20;
					logger.info("create success check thread");
					while (true) {
						try {
//                            ?????? ?????? ??????. ?????? ???????????? ???????????? ON
							if ((job != null && job.getStopSignal() != null && job.getStopSignal())
									|| (job != null && preProcess)) {
								logger.info(job != null && job.getStopSignal() != null && job.getStopSignal() ? "STOP SIGNAL" : "PREPROCESS DONE");
								logger.info("????????? ?????? ??????... 10s");
								Thread.sleep(10 * 1000);
								enableAutoDynamic();
								break;
							}
//                            ?????? ?????? ?????? ??????
							logger.info("?????? ?????? URL: {}", autoDynamicCheckUrl);
							ResponseEntity<String> searchCheckResponse = restTemplate.exchange(autoDynamicCheckUrl,
									HttpMethod.GET,
									new HttpEntity(new HashMap<String, Object>()),
									String.class
							);
							String status = null;
							try {
								Map<String, Object> body = gson.fromJson(searchCheckResponse.getBody(), Map.class);
								Map<String, Object> info = gson.fromJson(gson.toJson(body.get("info")), Map.class);
								status = String.valueOf(info.get("status"));
							}catch (Exception ignore) {}
//                            ????????? ???????????? ???????????? ON
							if ("SUCCESS".equalsIgnoreCase(status) || "NOT_STARTED".equalsIgnoreCase(status)) {
								logger.info("????????? ?????? ??????... 10s");
								Thread.sleep(10 * 1000);
								enableAutoDynamic();
								logger.info("[{}] autoDynamic >>> Open <<<", autoDynamicIndex);
								break;
							}
							r --;
							if (r == 0) {
								logger.warn("max retry!!!!");
								break;
							}
							Thread.sleep(60 * 1000);
						} catch (Exception e) {
							logger.error("", e);
							r --;
							if (r == 0) {
								logger.warn("max retry!!!!");
								break;
							} else {
								try {
									Thread.sleep(1000);
								} catch (InterruptedException ignore) {}
							}
						}
					}
					logger.info("success check Thread terminate");
				}).start();
			}
		}
	}

	public void updateQueueIndexerConsume(boolean dryRun, String queueIndexerUrl, String queueName, int consumeCount) {
		Map<String, Object> body = new HashMap<>();
		body.put("queue", queueName);
		body.put("size", consumeCount);
		logger.info("QueueIndexUrl: {}, queue: {}, count: {}", queueIndexerUrl, queueName, consumeCount);
		if (!dryRun) {
			ResponseEntity<String> response = restTemplate.exchange(queueIndexerUrl,
					HttpMethod.PUT,
					new HttpEntity(body),
					String.class
					);
			logger.info("edit Consume Response: {}", response);
		} else {
			logger.info("[DRY_RUN] queue indexer request skip");
		}
	}
	// ???????????? ON
	public void enableAutoDynamic() throws InterruptedException {
		if (autoDynamicQueueIndexUrl.split(",").length != 1) {
			// ?????? MQ
			for (int i = 0; i < autoDynamicQueueIndexUrl.split(",").length; i++) {
				String queueIndexUrl = autoDynamicQueueIndexUrl.split(",")[i];
				String queueName = autoDynamicQueueNames.get(i);
				updateQueueIndexerConsume(false, queueIndexUrl, queueName, autoDynamicQueueIndexConsumeCount);
				Thread.sleep(1000);
			}
		} else {
			// ?????? MQ
			for (String autoDynamicQueueName : autoDynamicQueueNames) {
				updateQueueIndexerConsume(false, autoDynamicQueueIndexUrl, autoDynamicQueueName, autoDynamicQueueIndexConsumeCount);
				Thread.sleep(1000);
			}
		}
	}

	// ???????????? OFF
	public void disableAutoDynamic(Map<String, Object> payload) throws InterruptedException {
		autoDynamicQueueNames = Arrays.asList(((String) payload.getOrDefault("autoDynamicQueueNames","")).split(","));
		autoDynamicCheckUrl = (String) payload.getOrDefault("autoDynamicCheckUrl","");
		autoDynamicQueueIndexUrl = (String) payload.getOrDefault("autoDynamicQueueIndexUrl","");
		try {
			autoDynamicQueueIndexConsumeCount = (int) payload.getOrDefault("autoDynamicQueueIndexConsumeCount",1);
		} catch (Exception ignore) {
			autoDynamicQueueIndexConsumeCount = Integer.parseInt((String) payload.getOrDefault("autoDynamicQueueIndexConsumeCount","1"));
		}
		// ??? ????????? ????????? ??? ??????.
		if (autoDynamicQueueIndexUrl.split(",").length != 1) {
			// ?????? MQ
			for (int i = 0; i < autoDynamicQueueIndexUrl.split(",").length; i++) {
				String queueIndexUrl = autoDynamicQueueIndexUrl.split(",")[i];
				String queueName = autoDynamicQueueNames.get(i);
				updateQueueIndexerConsume(false, queueIndexUrl, queueName, 0);
				Thread.sleep(1000);
			}
		} else {
			// ?????? MQ
			for (String autoDynamicQueueName : autoDynamicQueueNames) {
				updateQueueIndexerConsume(false, autoDynamicQueueIndexUrl, autoDynamicQueueName, 0);
				Thread.sleep(1000);
			}
		}
		logger.info("[{}] autoDynamic >>> Close <<<", autoDynamicQueueNames);
	}

	//      FIXME 20210618 ????????? - ???????????? ???????????? ???????????? remoteCmd ?????? ?????? (?????? ?????? )
	private void remoteCmd(String action, int retry) {
		String url = String.format("%s?action=%s", remoteCmdUrl, action);
		logger.info("REMOTE-CMD isCall: {}, URL: {}", enableRemoteCmd, url);
		if (!enableRemoteCmd) {
			return;
		}
		try {
			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
			ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(headers), String.class);
			logger.info("REMOTE-CMD Action: {} response status code: {}", action, responseEntity.getStatusCodeValue());
		} catch (Exception e) {
			logger.error("", e);
			Utils.sleep(3000);
			remoteCmd(action, retry - 1);
		}
	}

	private void officeLinkIndexing(String url, String groupSeqStr) {
		logger.info("office-link indexing start");
		url += "&action=all";
		url += "&groupSeq=" + groupSeqStr;
		try {
			logger.info("office-link indexing start - {}", url);
			restTemplate.exchange(url, HttpMethod.GET, new HttpEntity(new HashMap<>()), String.class);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

}