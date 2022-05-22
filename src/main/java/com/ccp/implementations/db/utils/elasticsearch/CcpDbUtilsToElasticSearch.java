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
	public CcpMapDecorator executeHttpRequest(int expectedStatus, String url, String method, CcpMapDecorator headers, String body) {
		headers = this.getHeaders().putAll(headers);
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(url, "POST", headers, body);

		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(int expectedStatus, String[] resourcesNames, String complemento, String method, CcpMapDecorator body) {
		String url = this.getUrl(resourcesNames, complemento);
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(url, "POST", headers, body);
		return executeHttpRequest;
	}

	private String getUrl(String[] resourcesNames, String complemento) {
		return this.getHeaders().getAsString("DB_URL") +  Arrays.asList(resourcesNames).stream().toString()
				.replace("[", "").replace("]", "").replace(" ", "") + complemento;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(CcpMapDecorator flows, String complemento, String method, CcpMapDecorator body) {
		CcpHttpHandler http = new CcpHttpHandler(flows, this.ccpHttp);
		CcpMapDecorator headers = this.getHeaders();
		String url = headers.getAsString("DB_URL") + complemento;
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(url, "POST", headers, body);
		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(String url, String method, CcpMapDecorator body, CcpMapDecorator flows) {
		CcpMapDecorator headers = this.getHeaders();
		CcpHttpHandler http = new CcpHttpHandler(flows, this.ccpHttp);
		String path = headers.getAsString("DB_URL") + url;
		CcpMapDecorator executeHttpRequest = http.executeHttpRequest(path, "POST", headers, body);
		
		return executeHttpRequest;
	}

	@Override
	public CcpMapDecorator executeHttpRequest(int expectedStatus, String url, String method) {
		CcpHttpHandler http = new CcpHttpHandler(expectedStatus, this.ccpHttp);
		CcpMapDecorator headers = this.getHeaders();
		String path = headers.getAsString("DB_URL") + url;
		CcpMapDecorator response = http.executeHttpRequest(path, method, headers, new CcpMapDecorator());
		return response;
	}

	
}
