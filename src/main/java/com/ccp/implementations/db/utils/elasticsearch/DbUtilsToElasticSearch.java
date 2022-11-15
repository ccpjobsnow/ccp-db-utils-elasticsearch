package com.ccp.implementations.db.utils.elasticsearch;

import java.util.Arrays;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpSpecification;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.http.CcpHttpResponseTransform;

class DbUtilsToElasticSearch implements CcpDbUtils {
	@CcpSpecification
	private CcpHttpRequester ccpHttp;

	private CcpMapDecorator getHeaders() {
		return new CcpMapDecorator()
				.put("Authorization", System.getenv("DB_CREDENTIALS"))
				.put("Content-Type", "application/json")
				.put("url", System.getenv("DB_URL"))
				.put("Accept", "application/json")
				;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method,  int expectedStatus, String body, CcpMapDecorator headers, CcpHttpResponseTransform<V> transformer) {
		headers = this.getHeaders().putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		V executeHttpRequest = http.executeHttpRequest(url, method, headers, body, transformer);

		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String complemento, String method, int expectedStatus, CcpMapDecorator body,  String[] resources, CcpHttpResponseTransform<V> transformer) {
		String path = this.getHeaders().getAsString("DB_URL") +  Arrays.asList(resources).stream().toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method, CcpMapDecorator flows, CcpMapDecorator body, CcpHttpResponseTransform<V> transformer) {
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(flows, this.ccpHttp);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	@Override
	public <V> V executeHttpRequest(String url, String method, int expectedStatus, CcpMapDecorator body, CcpHttpResponseTransform<V> transformer) {
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		String path = headers.getAsString("DB_URL") + url;
		V executeHttpRequest = http.executeHttpRequest(path, method, headers, body, transformer);
		
		return executeHttpRequest;
	}

	
}
