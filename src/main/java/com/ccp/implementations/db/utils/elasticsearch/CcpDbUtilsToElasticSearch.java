package com.ccp.implementations.db.utils.elasticsearch;

import java.util.Arrays;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpEspecification;
import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.http.CcpHttp;
import com.ccp.especifications.http.CcpHttpHandler;

@CcpImplementation
public class CcpDbUtilsToElasticSearch implements CcpDbUtils {
	@CcpEspecification
	private CcpHttp ccpHttp;

	private CcpMapDecorator getHeaders() {
		return new CcpMapDecorator()
				.put("Authorization", System.getenv("DB_CREDENTIALS"))
				.put("Content-Type", "application/json")
				.put("url", System.getenv("DB_URL"))
				.put("Accept", "application/json")
				;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(String url, String method,  int expectedStatus, String body, CcpMapDecorator headers) {
		headers = this.getHeaders().putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(url, method, headers, body);

		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(String complemento, String method, int expectedStatus, CcpMapDecorator body,  String[] resources) {
		String path = this.getHeaders().getAsString("DB_URL") +  Arrays.asList(resources).stream().toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(path, method, headers, body);
		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(String url, String method, CcpMapDecorator flows, CcpMapDecorator body) {
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(flows, this.ccpHttp);
		String path = headers.getAsString("DB_URL") + url;
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(path, method, headers, body);
		
		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(String url, String method, int expectedStatus, CcpMapDecorator body) {
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		String path = headers.getAsString("DB_URL") + url;
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(path, method, headers, body);
		
		return executeHttpRequest;
	}

	
}
