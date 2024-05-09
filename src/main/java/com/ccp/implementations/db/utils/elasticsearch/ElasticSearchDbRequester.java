package com.ccp.implementations.db.utils.elasticsearch;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpFolderDecorator;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.bulk.CcpBulkItem;
import com.ccp.especifications.db.bulk.CcpBulkOperationResult;
import com.ccp.especifications.db.bulk.CcpDbBulkExecutor;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.http.CcpHttpResponseTransform;
import com.ccp.exceptions.process.CcpMissingInputStream;
class ElasticSearchDbRequester implements CcpDbRequester {

	private CcpJsonRepresentation connectionDetails = CcpConstants.EMPTY_JSON;
	

	private void loadConnectionProperties() {
		boolean alreadyLoaded = this.connectionDetails.isEmpty() == false;
		if(alreadyLoaded) {
			return;
		}
		CcpJsonRepresentation systemProperties;
		try {
			CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("application_properties");
			CcpPropertiesDecorator propertiesFrom = ccpStringDecorator.propertiesFrom();
			systemProperties = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		} catch (CcpMissingInputStream e) {
			systemProperties = CcpConstants.EMPTY_JSON
					.put("elasticsearch.address", "http://localhost:9200")
					.put("elasticsearch.secret", "")
					;
		}
		
		CcpJsonRepresentation putIfNotContains = systemProperties
		.putIfNotContains("elasticsearch.address", "http://localhost:9200")
		.putIfNotContains("elasticsearch.secret", "");

		CcpJsonRepresentation subMap = putIfNotContains.getJsonPiece("elasticsearch.address", "elasticsearch.secret")
				.renameKey("elasticsearch.address", "DB_URL").renameKey("elasticsearch.secret", "Authorization")
				;
		
		this.connectionDetails = subMap
				.put("Content-Type", "application/json")
				.put("Accept", "application/json")
				;
	}

	
	public <V> V executeHttpRequest(String trace, String url, String method,  Integer expectedStatus, String body, CcpJsonRepresentation headers, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();;
		headers = this.connectionDetails.putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = this.connectionDetails.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String complemento, String method, Integer expectedStatus, CcpJsonRepresentation body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		String path = this.connectionDetails.getAsString("DB_URL") + "/" +  Arrays.asList(resources).stream()
				.collect(Collectors.toList())
				.toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String url, String method, CcpJsonRepresentation flows, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(flows);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String trace, String url, String method, Integer expectedStatus, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		this.loadConnectionProperties();
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(trace, path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getConnectionDetails() {
		this.loadConnectionProperties();
		return this.connectionDetails;
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<CcpBulkOperationResult> executeDatabaseSetup(String pathToJavaClasses, String hostFolder, Consumer<Throwable> whenOccursAnError) {
		CcpHttpRequester http = CcpDependencyInjection.getDependency(CcpHttpRequester.class);
		CcpFolderDecorator folderJava = new CcpStringDecorator(pathToJavaClasses).folder();
		List<CcpBulkItem> bulkItems = new ArrayList<>();
		folderJava.readFiles(x -> {
			String name = new File(x.content).getName();
			if("base".equals(name)) {
				return;
			}
			String replace = name.replace(".java", "");
			String[] split = pathToJavaClasses.split(hostFolder);
			String sourceFolder = split[split.length - 1];
			String packageName = sourceFolder.replace("\\", ".").replace("/", ".");
			
			String className = packageName + "." + replace;
			Constructor<CcpEntity> declaredConstructor;
			try {
				declaredConstructor = (Constructor<CcpEntity>) Class.forName(className).getDeclaredConstructor();
				declaredConstructor.setAccessible(true);
				CcpEntity newInstance = declaredConstructor.newInstance();
				String scriptToCreateEntity = newInstance.getScriptToCreateEntity();
				String entityName = newInstance.getEntityName();
				
				String dbUrl = this.connectionDetails.getAsString("DB_URL");
				
				String urlToEntity = dbUrl + "/" + entityName;
				http.executeHttpRequest(urlToEntity, "DELETE", this.connectionDetails, scriptToCreateEntity, 200);
				http.executeHttpRequest(urlToEntity, "PUT", this.connectionDetails, scriptToCreateEntity, 200);
				List<CcpBulkItem> firstRecordsToInsert = newInstance.getFirstRecordsToInsert();
				bulkItems.addAll(firstRecordsToInsert);
				
			} catch (Exception e) {
				whenOccursAnError.accept(e);
			}

		});	
		CcpDbBulkExecutor bulk = CcpDependencyInjection.getDependency(CcpDbBulkExecutor.class);
		bulk.addRecords(bulkItems);
		List<CcpBulkOperationResult> bulkOperationResult = bulk.getBulkOperationResult();
		return bulkOperationResult;
	}
	
	
}
