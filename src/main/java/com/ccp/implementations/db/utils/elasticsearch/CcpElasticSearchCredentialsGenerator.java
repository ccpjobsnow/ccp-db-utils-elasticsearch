package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.db.utils.CcpDbCredentialsGenerator;

@CcpImplementation
public class CcpElasticSearchCredentialsGenerator implements CcpDbCredentialsGenerator {

	@Override
	public CcpMapDecorator getDatabaseCredentials() {
		return new CcpMapDecorator()
				.put("Authorization", System.getenv("DB_CREDENTIALS"))
				.put("Content-Type", "application/json")
				.put("url", System.getenv("DB_URL"))
				.put("Accept", "application/json")
				;
	}

}
