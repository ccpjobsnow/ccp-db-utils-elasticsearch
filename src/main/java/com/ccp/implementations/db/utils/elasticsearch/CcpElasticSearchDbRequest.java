package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class CcpElasticSearchDbRequest implements CcpInstanceProvider {

	
	public Object getInstance() {
		return new ElasticSearchDbRequester();
	}

}
