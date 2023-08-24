package com.ccp.implementations.db.utils.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class DbUtils implements CcpInstanceProvider {

	@Override
	public Object getInstance() {
		return new DbUtilsToElasticSearch();
	}

}
