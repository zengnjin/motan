/*
 * Copyright (C) 2016 Weibo, Inc. All Rights Reserved.
 */

package com.weibo.api.motan.cluster.ha;

import java.util.ArrayList;
import java.util.List;

import org.jmock.Expectations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.weibo.api.motan.BaseTestCase;
import com.weibo.api.motan.cluster.LoadBalance;
import com.weibo.api.motan.common.MotanConstants;
import com.weibo.api.motan.common.URLParamType;
import com.weibo.api.motan.exception.MotanServiceException;
import com.weibo.api.motan.protocol.example.IHello;
import com.weibo.api.motan.protocol.example.IWorld;
import com.weibo.api.motan.rpc.DefaultRequest;
import com.weibo.api.motan.rpc.Referer;
import com.weibo.api.motan.rpc.Request;
import com.weibo.api.motan.rpc.Response;
import com.weibo.api.motan.rpc.URL;
import com.weibo.api.motan.util.NetUtils;

/**
 * 
 * BackupRequest ha strategy test
 *
 * @author leijian
 */

public class BackupRequestHaStrategyTest extends BaseTestCase{

	private BackupRequestHaStrategy<IWorld> backupRequestHaStrategy;
	private List<Referer<IWorld>> referers = null;
	private LoadBalance<IWorld> loadBalance = null;
	private int retries = 2;
	
	@Before
	@Override
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception{
		super.setUp();
		loadBalance = mockery.mock(LoadBalance.class);
		final Referer<IWorld> referer1 = mockery.mock(Referer.class, "ref1");
		final Referer<IWorld> referer2 = mockery.mock(Referer.class, "ref2");
		referers = new ArrayList<Referer<IWorld>>();
		referers.add(referer1);
		referers.add(referer2);
		backupRequestHaStrategy = new BackupRequestHaStrategy<IWorld>(){
			@Override
			protected List<Referer<IWorld>> selectReferers(Request request,
					LoadBalance<IWorld> loadBalance) {
				return referers;
			}
		};
		URL url = new URL(MotanConstants.PROTOCOL_MOTAN, NetUtils.LOCALHOST, 0, IWorld.class.getName());
		url.addParameter(URLParamType.retries.getName(), String.valueOf(retries));
		backupRequestHaStrategy.setUrl(url);
	}
	
	@Test
	public void testCall(){
        final DefaultRequest request = new DefaultRequest();
        request.setMethodName(IWorld.class.getMethods()[0].getName());
        request.setArguments(new Object[]{});
        request.setInterfaceName(IHello.class.getSimpleName());
        request.setParamtersDesc("void");
        final Response response = mockery.mock(Response.class);
        final URL url = URL.valueOf("motan%3A%2F%2F10.209.128.244%3A8000%2Fcom.weibo.api.motan.procotol.example.IWorld%3Fprotocol%3Dmotan%26export%3Dmotan%3A8000%26application%3Dapi%26module%3Dtest%26check%3Dtrue%26refreshTimestamp%3D1373275099717%26methodconfig.world%28void%29.retries%3D1%26id%3Dmotan%26methodconfig.world%28java.lang.String%29.retries%3D1%26methodconfig.world%28java.lang.String%2Cboolean%29.retries%3D1%26nodeType%3Dservice%26group%3Dwangzhe-test-yf%26shareChannel%3Dtrue%26&");
		
		mockery.checking(new Expectations(){{
//			one(loadBalance).selectToHolder(request, backupRequestHaStrategy.referersHolder.get());
	          for(Referer<IWorld> ref : referers){
	                atLeast(0).of(ref).call(request);will(returnValue(response));
	                atLeast(0).of(ref).isAvailable();will(returnValue(true));
	                atLeast(0).of(ref).getUrl();will(returnValue(url));
	                atLeast(1).of(ref).destroy();
	            }

			one(referers.get(0)).call(request);will(returnValue(response));
		}});
		backupRequestHaStrategy.call(request, loadBalance);
	}
	
	public void testCallWithOneError(){
        final DefaultRequest request = new DefaultRequest();
        request.setMethodName(IWorld.class.getMethods()[0].getName());
        request.setArguments(new Object[]{});
        request.setInterfaceName(IHello.class.getSimpleName());
        request.setParamtersDesc("void");
        final Response response = mockery.mock(Response.class);
        final URL url = URL.valueOf("motan%3A%2F%2F10.209.128.244%3A8000%2Fcom.weibo.api.motan.procotol.example.IWorld%3Fprotocol%3Dmotan%26export%3Dmotan%3A8000%26application%3Dapi%26module%3Dtest%26check%3Dtrue%26refreshTimestamp%3D1373275099717%26methodconfig.world%28void%29.retries%3D1%26id%3Dmotan%26methodconfig.world%28java.lang.String%29.retries%3D1%26methodconfig.world%28java.lang.String%2Cboolean%29.retries%3D1%26nodeType%3Dservice%26group%3Dwangzhe-test-yf%26shareChannel%3Dtrue%26&");
        
        mockery.checking(new Expectations(){{
//          one(loadBalance).selectToHolder(request, backupRequestHaStrategy.referersHolder.get());
              for(Referer<IWorld> ref : referers){
                    atLeast(0).of(ref).call(request);will(returnValue(response));
                    atLeast(0).of(ref).isAvailable();will(returnValue(true));
                    atLeast(0).of(ref).getUrl();will(returnValue(url));
                    atLeast(1).of(ref).destroy();
                }
              one(referers.get(0)).call(request);will(throwException(new MotanServiceException("mock throw exception when call")));
              one(referers.get(1)).call(request);will(returnValue(response));
		}});
		
		backupRequestHaStrategy.call(request, loadBalance);
	}
	
	public void testCallWithFalse(){
        final DefaultRequest request = new DefaultRequest();
        request.setMethodName(IWorld.class.getMethods()[0].getName());
        request.setArguments(new Object[]{});
        request.setInterfaceName(IHello.class.getSimpleName());
        request.setParamtersDesc("void");
        final Response response = mockery.mock(Response.class);
        final URL url = URL.valueOf("motan%3A%2F%2F10.209.128.244%3A8000%2Fcom.weibo.api.motan.procotol.example.IWorld%3Fprotocol%3Dmotan%26export%3Dmotan%3A8000%26application%3Dapi%26module%3Dtest%26check%3Dtrue%26refreshTimestamp%3D1373275099717%26methodconfig.world%28void%29.retries%3D1%26id%3Dmotan%26methodconfig.world%28java.lang.String%29.retries%3D1%26methodconfig.world%28java.lang.String%2Cboolean%29.retries%3D1%26nodeType%3Dservice%26group%3Dwangzhe-test-yf%26shareChannel%3Dtrue%26&");
        
        mockery.checking(new Expectations(){{
          one(loadBalance).selectToHolder(request, backupRequestHaStrategy.referersHolder.get());
              for(Referer<IWorld> ref : referers){
                    atLeast(0).of(ref).isAvailable();will(returnValue(true));
                    atLeast(0).of(ref).getUrl();will(returnValue(url));
                    atLeast(0).of(ref).destroy();
                }
              atLeast(2).of(referers.get(0)).call(request);will(throwException(new MotanServiceException("mock throw exception when 1th call")));
              oneOf(referers.get(1)).call(request);will(throwException(new MotanServiceException("mock throw exception when 2th call")));
		}});
		
		try {
			backupRequestHaStrategy.call(request, loadBalance);
			fail("Should throw exception before!");
			Assert.assertTrue(false); //should not run to here
		} catch (Exception e) {
		}
	}
}