package com.ccp.implementations.db.utils.elasticsearch;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpCollectionDecorator;
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
import com.ccp.especifications.db.utils.CcpEntityField;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.http.CcpHttpResponseTransform;
import com.ccp.exceptions.db.CcpIncorrectEntityFields;
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

	@SuppressWarnings("unchecked")
	public List<CcpBulkOperationResult> executeDatabaseSetup(String pathToJavaClasses, String hostFolder, String pathToCreateEntityScript,	Consumer<CcpIncorrectEntityFields> whenTheFieldsInTheEntityAreIncorrect,	Consumer<Throwable> whenOccursAnUnhadledError) {
		this.loadConnectionProperties();
		CcpHttpRequester http = CcpDependencyInjection.getDependency(CcpHttpRequester.class);
		CcpFolderDecorator folderJava = new CcpStringDecorator(pathToJavaClasses).folder();
		List<CcpBulkItem> bulkItems = new ArrayList<>();
		folderJava.readFiles(x -> {
			String name = new File(x.content).getName();
			String replace = name.replace(".java", "");
			String[] split = pathToJavaClasses.split(hostFolder);
			String sourceFolder = split[split.length - 1];
			String packageName = sourceFolder.replace("\\", ".").replace("/", ".");
			if(packageName.startsWith(".")) {
				packageName = packageName.substring(1);
			}
			String className = packageName + "." + replace;
			
			Constructor<CcpEntity> declaredConstructor;
			try {
				declaredConstructor = (Constructor<CcpEntity>) Class.forName(className).getDeclaredConstructor();
				declaredConstructor.setAccessible(true);
				CcpEntity entity = declaredConstructor.newInstance();
				String entityName = entity.getEntityName();
				String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityName);
				
				this.validateEntityFields(entity, pathToCreateEntityScript);
				
				String dbUrl = this.connectionDetails.getAsString("DB_URL");
				
				String urlToEntity = dbUrl + "/" + entityName;
				this.recreateEntity(http, scriptToCreateEntity, urlToEntity);
				this.recreateEntityMirror(http, entity, scriptToCreateEntity, dbUrl);
				List<CcpBulkItem> firstRecordsToInsert = entity.getFirstRecordsToInsert();
				bulkItems.addAll(firstRecordsToInsert);
			}catch(CcpIncorrectEntityFields e) {
				whenTheFieldsInTheEntityAreIncorrect.accept(e);
			}catch (Throwable e) {
				whenOccursAnUnhadledError.accept(e);
			}

		});	
		CcpDbBulkExecutor bulk = CcpDependencyInjection.getDependency(CcpDbBulkExecutor.class);
		bulk.addRecords(bulkItems);
		List<CcpBulkOperationResult> bulkOperationResult = bulk.getBulkOperationResult();
		return bulkOperationResult;
	}


	private void recreateEntityMirror(CcpHttpRequester http, CcpEntity entity, String scriptToCreateEntity, String dbUrl) {
		
		boolean hasNoMirrorEntity = entity.hasMirrorEntity() == false;
		
		if(hasNoMirrorEntity) {
			return;
		}
		CcpEntity mirrorEntity = entity.getMirrorEntity();
		String entityNameMirror = mirrorEntity.getEntityName();
		String urlToEntityMirror = dbUrl + "/" + entityNameMirror;
		this.recreateEntity(http, scriptToCreateEntity, urlToEntityMirror);
	}


	private void recreateEntity(CcpHttpRequester http, String scriptToCreateEntity, String urlToEntity) {
		http.executeHttpRequest(urlToEntity, "DELETE", this.connectionDetails, scriptToCreateEntity, 200, 404);
		http.executeHttpRequest(urlToEntity, "PUT", this.connectionDetails, scriptToCreateEntity, 200);
	}

	private String getScriptToCreateEntity(String pathToCreateEntityScript, String entityName) {
		String createEntityFile = pathToCreateEntityScript + "/" + entityName;
		String scriptToCreateEntity = new CcpStringDecorator(createEntityFile).file().extractStringContent();
		return scriptToCreateEntity;
	}
	
	private void validateEntityFields(CcpEntity entity, String pathToCreateEntityScript) {
		
		String entityName = entity.getEntityName();
		String scriptToCreateEntity = this.getScriptToCreateEntity(pathToCreateEntityScript, entityName);
		CcpJsonRepresentation scriptToCreateEntityAsJson = new CcpJsonRepresentation(scriptToCreateEntity);
		CcpJsonRepresentation mappings = scriptToCreateEntityAsJson.getInnerJson("mappings");
		String dynamic = mappings.getAsString("dynamic");
		
		boolean isNotStrict = "strict".equals(dynamic) == false;
		
		if(isNotStrict) {
			String messageError = String.format("The entity '%s' does not have the dynamic properties equals to strict. The script to this entity is %s", dynamic, scriptToCreateEntityAsJson);
			throw new CcpIncorrectEntityFields(messageError);
		}
		
		CcpJsonRepresentation propertiesJson = mappings.getInnerJson("properties");
		Set<String> scriptFields = propertiesJson.keySet();
		CcpEntityField[] fields = entity.getFields();
		List<String> classFields = Arrays.asList(fields).stream().map(x -> x.name()).collect(Collectors.toList());
		List<String> isInClassButIsNotInScript = new CcpCollectionDecorator((Object[])scriptFields.toArray(new String[scriptFields.size()])).getExclusiveList(classFields);
		List<String> isInScriptButIsNotInClass = new CcpCollectionDecorator((Object[])classFields.toArray(new String[classFields.size()])).getExclusiveList(scriptFields);
		
		String className = entity.getClass().getSimpleName();
		String messageError = String.format("The class '%s'\n that belongs to the entity '%s'\n has an incorrect mapping, "
				+ "fields that are in script but are not in class %s,\n "
				+ "fields that are in class but are not in script %s.\n "
				+ "The script to this entity is %s", className, entityName, isInClassButIsNotInScript, 
				isInScriptButIsNotInClass, scriptToCreateEntityAsJson);
		boolean missingsInClass = isInScriptButIsNotInClass.isEmpty() == false;
		
		if(missingsInClass) {
			throw new CcpIncorrectEntityFields(messageError);
		}
		
		boolean missingsInScript = isInClassButIsNotInScript.isEmpty() == false;

		if(missingsInScript) {
			throw new CcpIncorrectEntityFields(messageError);
		}
	}

}
