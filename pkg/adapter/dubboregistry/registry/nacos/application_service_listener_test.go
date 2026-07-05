/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nacos

import (
	"net/http"
	"strings"
	"testing"
)

import (
	dubboConst "dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/metadata/info"
	dr "dubbo.apache.org/dubbo-go/v3/registry"

	nacosModel "github.com/nacos-group/nacos-sdk-go/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	registrycommon "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/common"
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/filter/http/remote"
)

func TestAppendServiceProjectionCreatesUniqueSignatureDynamicAPI(t *testing.T) {
	listener := newNacosAppSrvListener(nil, nil, "nacos-main", "DemoProviderApp")
	instance := &dr.DefaultServiceInstance{
		ID:          "instance-1",
		ServiceName: "DemoProviderApp",
		Host:        "10.0.0.8",
		Port:        20880,
		Metadata:    map[string]string{},
	}
	service := testMetadataService(map[string]string{
		constant.MethodsKey:       "SayHello",
		"SayHello.parameterTypes": `["java.lang.String"]`,
		constant.ApplicationKey:   "DemoProviderApp",
		constant.VersionKey:       "1.0.0",
		constant.GroupKey:         "dubbo-group",
		constant.SerializationKey: "hessian2",
		dubboConst.TimeoutKey:     "3000",
	})
	projection := &instanceProjection{
		key: registrycommon.OwnerKey{
			RegistryID:             "nacos-main",
			ApplicationServiceName: "DemoProviderApp",
			InstanceIdentity:       "instance-1",
		},
		record: registrycommon.ProjectionRecord{
			Services: map[string]registrycommon.ServiceProjection{},
		},
	}

	listener.appendServiceProjection(projection, instance, service)

	clusterName := "DemoProviderApp/org.apache.demo.Greeter/1.0.0/dubbo-group"
	require.Len(t, projection.endpoints, 1)
	assert.Equal(t, clusterName+"|10.0.0.8:20880|tri|hessian2", projection.endpoints[0].ID)
	assert.Equal(t, "10.0.0.8", projection.endpoints[0].Address.Address)
	assert.Equal(t, 20880, projection.endpoints[0].Address.Port)
	assert.Equal(t, map[string]string{
		constant.ApplicationKey:   "DemoProviderApp",
		constant.InterfaceKey:     "org.apache.demo.Greeter",
		constant.VersionKey:       "1.0.0",
		constant.GroupKey:         "dubbo-group",
		"protocol":                "tri",
		constant.SerializationKey: "hessian2",
		dubboConst.TimeoutKey:     "3000",
	}, projection.endpoints[0].Metadata)

	require.Len(t, projection.apis, 1)
	api := projection.apis[0]
	assert.Equal(t, "/DemoProviderApp/org.apache.demo.Greeter/SayHello", api.URLPattern)
	assert.Equal(t, constant.Post, api.HTTPVerb)
	assert.Equal(t, constant.DubboRequest, api.IntegrationRequest.RequestType)
	assert.Empty(t, api.IntegrationRequest.HTTPBackendConfig.URL)
	assert.Equal(t, "SayHello", api.IntegrationRequest.Method)
	assert.Equal(t, []string{"java.lang.String"}, api.IntegrationRequest.ParameterTypes)
	require.Len(t, api.IntegrationRequest.MappingParams, 1)
	assert.Equal(t, "requestBody.values", api.IntegrationRequest.MappingParams[0].Name)
	assert.Equal(t, "opt.values", api.IntegrationRequest.MappingParams[0].MapTo)

	require.Len(t, projection.routers, 1)
	assert.Equal(t, "/DemoProviderApp/org.apache.demo.Greeter/SayHello", projection.routers[0].ID)
	assert.Equal(t, "/DemoProviderApp/org.apache.demo.Greeter/SayHello", projection.routers[0].Match.Path)
	assert.Equal(t, []string{constant.Post}, projection.routers[0].Match.Methods)
	assert.Equal(t, clusterName, projection.routers[0].Route.Cluster)

	serviceProjection, ok := projection.record.Services[clusterName]
	require.True(t, ok)
	assert.Equal(t, clusterName, serviceProjection.ClusterName)
	assert.Equal(t, []string{projection.endpoints[0].ID}, serviceProjection.EndpointIDs)
	assert.Equal(t, []string{projection.routers[0].ID}, serviceProjection.RouterIDs)
	assert.Equal(t, []registrycommon.APIKey{
		{Path: "/DemoProviderApp/org.apache.demo.Greeter/SayHello", HTTPMethod: constant.Post},
	}, serviceProjection.APIKeys)

	req, err := http.NewRequest(constant.Post, api.URLPattern, strings.NewReader(`{"values":["neo"]}`))
	require.NoError(t, err)
	outbound, err := (&remote.DubboHandler{}).BuildOutbound(req, api, projection.endpoints[0])
	require.NoError(t, err)
	assert.Equal(t, "org.apache.demo.Greeter", outbound.Service)
	assert.Equal(t, "10.0.0.8:20880", outbound.Address)
	assert.Equal(t, "tri", outbound.Protocol)
	assert.Equal(t, "hessian2", outbound.Serialization)
	assert.Equal(t, "SayHello", outbound.Method)
	assert.Equal(t, []string{"java.lang.String"}, outbound.ParamTypes)
	assert.Equal(t, []any{"neo"}, outbound.Arguments)
}

func TestAppendServiceProjectionSkipsMissingCompleteSignature(t *testing.T) {
	listener := newNacosAppSrvListener(nil, nil, "nacos-main", "DemoProviderApp")
	instance := &dr.DefaultServiceInstance{
		ServiceName: "DemoProviderApp",
		Host:        "10.0.0.8",
		Port:        20880,
		Metadata:    map[string]string{},
	}
	service := testMetadataService(map[string]string{
		constant.MethodsKey:       "SayHello",
		constant.SerializationKey: "hessian2",
	})
	projection := &instanceProjection{
		record: registrycommon.ProjectionRecord{
			Services: map[string]registrycommon.ServiceProjection{},
		},
	}

	listener.appendServiceProjection(projection, instance, service)

	assert.Empty(t, projection.endpoints)
	assert.Empty(t, projection.apis)
	assert.Empty(t, projection.routers)
	assert.Empty(t, projection.record.Services)
}

func TestInstanceIdentityPrefersInstanceID(t *testing.T) {
	assert.Equal(t, "instance-1", instanceIdentity(nacosModel.Instance{
		InstanceId: " instance-1 ",
		Ip:         "10.0.0.8",
		Port:       20880,
	}))
	assert.Equal(t, "10.0.0.8:20880", instanceIdentity(nacosModel.Instance{
		Ip:   "10.0.0.8",
		Port: 20880,
	}))
}

func testMetadataService(params map[string]string) *info.ServiceInfo {
	return info.NewServiceInfo(
		"org.apache.demo.Greeter",
		params[constant.GroupKey],
		params[constant.VersionKey],
		"tri",
		"org.apache.demo.Greeter",
		params,
	)
}
