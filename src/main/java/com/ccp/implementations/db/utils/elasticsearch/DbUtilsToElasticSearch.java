package com.ccp.implementations.db.utils.elasticsearch;

import java.util.Arrays;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.http.CcpHttpResponseTransform;

class DbUtilsToElasticSearch implements CcpDbUtils {
	@CcpDependencyInject
	private CcpHttpRequester ccpHttp;

	private CcpMapDecorator connectionDetails = new CcpMapDecorator();
	
	public DbUtilsToElasticSearch() {
		CcpMapDecorator systemProperties;
		try {
			systemProperties = new CcpStringDecorator("application1.properties").propertiesFileFromClassLoader();
		} catch (Exception e) {
			systemProperties = new CcpMapDecorator();
		}
		Object url = systemProperties.getOrDefault("elasticsearch.address", "http://localhost:9200");
		Object secret = systemProperties.getOrDefault("elasticsearch.secret", "");
		
		this.connectionDetails = this.connectionDetails.put("Content-Type", "application/json")
				.put("Content-Type", "application/json").put("DB_URL", url).put("Authorization", secret)
				;
	}
	

	@Override
	public <V> V executeHttpRequest(String url, String method,  Integer expectedStatus, String body, CcpMapDecorator headers, CcpHttpResponseTransform<V> transformer) {
		headers = this.connectionDetails.putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		V executeHttpRequest = http.executeHttpRequest(url, method, headers, body, transformer);

		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String complemento, String method, Integer expectedStatus, CcpMapDecorator body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		String path = this.connectionDetails.getAsString("DB_URL") +  Arrays.asList(resources).stream().toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
		CcpMapDecorator headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method, CcpMapDecorator flows, CcpMapDecorator body, CcpHttpResponseTransform<V> transformer) {
		CcpMapDecorator headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(flows, this.ccpHttp);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method, Integer expectedStatus, CcpMapDecorator body, CcpHttpResponseTransform<V> transformer) {
		CcpMapDecorator headers = this.connectionDetails;
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	

	
}
