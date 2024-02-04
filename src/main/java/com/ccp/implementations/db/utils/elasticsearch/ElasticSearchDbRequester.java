package com.ccp.implementations.db.utils.elasticsearch;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpResponseTransform;
import com.ccp.exceptions.process.CcpMissingInputStream;
class ElasticSearchDbRequester implements CcpDbRequester {

	private CcpJsonRepresentation connectionDetails = CcpConstants.EMPTY_JSON;
	
	public ElasticSearchDbRequester() {
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

	
	public <V> V executeHttpRequest(String url, String method,  Integer expectedStatus, String body, CcpJsonRepresentation headers, CcpHttpResponseTransform<V> transformer) {
		headers = this.connectionDetails.putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = this.connectionDetails.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String complemento, String method, Integer expectedStatus, CcpJsonRepresentation body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		String path = this.connectionDetails.getAsString("DB_URL") + "/" +  Arrays.asList(resources).stream()
				.collect(Collectors.toList())
				.toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String url, String method, CcpJsonRepresentation flows, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(flows);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public <V> V executeHttpRequest(String url, String method, Integer expectedStatus, CcpJsonRepresentation body, CcpHttpResponseTransform<V> transformer) {
		CcpJsonRepresentation headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
	public CcpJsonRepresentation getConnectionDetails() {
		return this.connectionDetails;
	}


}
