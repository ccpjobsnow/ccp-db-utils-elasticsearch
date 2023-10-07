package com.ccp.implementations.db.utils.elasticsearch;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpResponseTransform;
import com.ccp.exceptions.process.CcpMissingInputStream;
class ElasticSearchDbRequester implements CcpDbRequester {

	private CcpMapDecorator connectionDetails = new CcpMapDecorator();
	
	public ElasticSearchDbRequester() {
		CcpMapDecorator systemProperties;
		try {
			CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("application.properties");
			CcpPropertiesDecorator propertiesFrom = ccpStringDecorator.propertiesFrom();
			systemProperties = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		} catch (CcpMissingInputStream e) {
			systemProperties = new CcpMapDecorator()
					.put("elasticsearch.address", "http://localhost:9200")
					.put("elasticsearch.secret", "")
					;
		}
		
		CcpMapDecorator putIfNotContains = systemProperties
		.putIfNotContains("elasticsearch.address", "http://localhost:9200")
		.putIfNotContains("elasticsearch.secret", "");

		CcpMapDecorator subMap = putIfNotContains.getSubMap("elasticsearch.address", "elasticsearch.secret")
				.renameKey("elasticsearch.address", "DB_URL").renameKey("elasticsearch.secret", "Authorization")
				;
		
		this.connectionDetails = subMap
				.put("Content-Type", "application/json")
				.put("Accept", "application/json")
				;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method,  Integer expectedStatus, String body, CcpMapDecorator headers, CcpHttpResponseTransform<V> transformer) {
		headers = this.connectionDetails.putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = this.connectionDetails.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String complemento, String method, Integer expectedStatus, CcpMapDecorator body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		String path = this.connectionDetails.getAsString("DB_URL") + "/" +  Arrays.asList(resources).stream()
				.collect(Collectors.toList())
				.toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
		CcpMapDecorator headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method, CcpMapDecorator flows, CcpMapDecorator body, CcpHttpResponseTransform<V> transformer) {
		CcpMapDecorator headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(flows);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method, Integer expectedStatus, CcpMapDecorator body, CcpHttpResponseTransform<V> transformer) {
		CcpMapDecorator headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator getConnectionDetails() {
		return this.connectionDetails;
	}


}
