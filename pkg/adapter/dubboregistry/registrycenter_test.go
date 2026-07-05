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

package dubboregistry

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	registrycommon "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/common"
	dubboRegistry "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry"
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/config"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	"github.com/apache/dubbo-go-pixiu/pkg/router"
	"github.com/apache/dubbo-go-pixiu/pkg/server"
)

type capturedAPIConfigListener struct {
	added   []router.API
	removed []router.API
}

func (l *capturedAPIConfigListener) OnAddAPI(r router.API) error {
	l.added = append(l.added, r)
	return nil
}

func (l *capturedAPIConfigListener) OnRemoveAPI(r router.API) error {
	l.removed = append(l.removed, r)
	return nil
}

func (l *capturedAPIConfigListener) OnDeleteRouter(config.Resource) error {
	return nil
}

type capturedRouterListener struct {
	added   []*model.Router
	deleted []*model.Router
}

func (l *capturedRouterListener) OnAddRouter(r *model.Router) {
	l.added = append(l.added, r)
}

func (l *capturedRouterListener) OnDeleteRouter(r *model.Router) {
	l.deleted = append(l.deleted, r)
}

func TestApplyNacosInstanceProjectionWritesManagersAndOwnership(t *testing.T) {
	fixture := newProjectionManagerFixture(t)
	adapter := newProjectionTestAdapter()
	key := testOwnerKey()
	clusterName, endpoint, route, api, record := testProjection("endpoint-1", "SayHello")

	err := adapter.ApplyNacosInstanceProjection(key, record, []*model.Endpoint{endpoint}, []*model.Router{route}, []router.API{api})

	require.NoError(t, err)
	store, err := fixture.clusterManager.CloneStore()
	require.NoError(t, err)
	require.Len(t, store.Config, 1)
	assert.Equal(t, clusterName, store.Config[0].Name)
	require.Len(t, store.Config[0].Endpoints, 1)
	assert.Equal(t, endpoint.ID, store.Config[0].Endpoints[0].ID)
	assert.Equal(t, endpoint.Metadata, store.Config[0].Endpoints[0].Metadata)
	require.Len(t, fixture.routeListener.added, 1)
	assert.Equal(t, route.ID, fixture.routeListener.added[0].ID)
	require.Len(t, fixture.apiListener.added, 1)
	assert.Equal(t, api.URLPattern, fixture.apiListener.added[0].URLPattern)
	loaded, ok := adapter.projectionOwnership.LoadProjection(key)
	require.True(t, ok)
	assert.Equal(t, record, loaded)
}

func TestApplyNacosInstanceProjectionDoesNotReAddSharedRouterAndAPI(t *testing.T) {
	fixture := newProjectionManagerFixture(t)
	adapter := newProjectionTestAdapter()
	key1 := testOwnerKeyForInstance("instance-1")
	key2 := testOwnerKeyForInstance("instance-2")
	clusterName, endpoint1, route1, api1, record1 := testProjection("endpoint-1", "SayHello")
	_, endpoint2, route2, api2, record2 := testProjection("endpoint-2", "SayHello")

	require.Equal(t, route1.ID, route2.ID)
	require.Equal(t, api1.URLPattern, api2.URLPattern)
	require.NoError(t, adapter.ApplyNacosInstanceProjection(key1, record1, []*model.Endpoint{endpoint1}, []*model.Router{route1}, []router.API{api1}))

	err := adapter.ApplyNacosInstanceProjection(key2, record2, []*model.Endpoint{endpoint2}, []*model.Router{route2}, []router.API{api2})

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{endpoint1.ID, endpoint2.ID}, endpointIDsForCluster(t, fixture.clusterManager, clusterName))
	require.Len(t, fixture.routeListener.added, 1)
	assert.Equal(t, route1.ID, fixture.routeListener.added[0].ID)
	require.Len(t, fixture.apiListener.added, 1)
	assert.Equal(t, api1.URLPattern, fixture.apiListener.added[0].URLPattern)
}

func TestApplyNacosInstanceProjectionIsIdempotentForSameOwnerRouterAndAPI(t *testing.T) {
	fixture := newProjectionManagerFixture(t)
	adapter := newProjectionTestAdapter()
	key := testOwnerKey()
	clusterName, endpoint, route, api, record := testProjection("endpoint-1", "SayHello")
	require.NoError(t, adapter.ApplyNacosInstanceProjection(key, record, []*model.Endpoint{endpoint}, []*model.Router{route}, []router.API{api}))

	err := adapter.ApplyNacosInstanceProjection(key, record, []*model.Endpoint{endpoint}, []*model.Router{route}, []router.API{api})

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{endpoint.ID}, endpointIDsForCluster(t, fixture.clusterManager, clusterName))
	require.Len(t, fixture.routeListener.added, 1)
	assert.Equal(t, route.ID, fixture.routeListener.added[0].ID)
	require.Len(t, fixture.apiListener.added, 1)
	assert.Equal(t, api.URLPattern, fixture.apiListener.added[0].URLPattern)
	assert.Empty(t, fixture.routeListener.deleted)
	assert.Empty(t, fixture.apiListener.removed)
}

func TestDeleteInstanceProjectionRemovesRuntimeResourcesAndOwnership(t *testing.T) {
	fixture := newProjectionManagerFixture(t)
	adapter := newProjectionTestAdapter()
	key := testOwnerKey()
	clusterName, endpoint, route, api, record := testProjection("endpoint-1", "SayHello")
	require.NoError(t, adapter.ApplyNacosInstanceProjection(key, record, []*model.Endpoint{endpoint}, []*model.Router{route}, []router.API{api}))

	err := adapter.DeleteInstanceProjection(key)

	require.NoError(t, err)
	assert.Empty(t, endpointIDsForCluster(t, fixture.clusterManager, clusterName))
	require.Len(t, fixture.routeListener.deleted, 1)
	assert.Equal(t, route.ID, fixture.routeListener.deleted[0].ID)
	require.Len(t, fixture.apiListener.removed, 1)
	assert.Equal(t, api.URLPattern, fixture.apiListener.removed[0].URLPattern)
	assert.Equal(t, constant.Post, fixture.apiListener.removed[0].HTTPVerb)
	_, ok := adapter.projectionOwnership.LoadProjection(key)
	assert.False(t, ok)
}

func TestDeleteInstanceProjectionKeepsSharedRouterAndAPIUntilLastOwner(t *testing.T) {
	fixture := newProjectionManagerFixture(t)
	adapter := newProjectionTestAdapter()
	key1 := testOwnerKeyForInstance("instance-1")
	key2 := testOwnerKeyForInstance("instance-2")
	clusterName, endpoint1, route1, api1, record1 := testProjection("endpoint-1", "SayHello")
	_, endpoint2, route2, api2, record2 := testProjection("endpoint-2", "SayHello")
	require.NoError(t, adapter.ApplyNacosInstanceProjection(key1, record1, []*model.Endpoint{endpoint1}, []*model.Router{route1}, []router.API{api1}))
	require.NoError(t, adapter.ApplyNacosInstanceProjection(key2, record2, []*model.Endpoint{endpoint2}, []*model.Router{route2}, []router.API{api2}))

	err := adapter.DeleteInstanceProjection(key1)

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{endpoint2.ID}, endpointIDsForCluster(t, fixture.clusterManager, clusterName))
	assert.Empty(t, fixture.routeListener.deleted)
	assert.Empty(t, fixture.apiListener.removed)
	_, ok := adapter.projectionOwnership.LoadProjection(key1)
	assert.False(t, ok)

	err = adapter.DeleteInstanceProjection(key2)

	require.NoError(t, err)
	assert.Empty(t, endpointIDsForCluster(t, fixture.clusterManager, clusterName))
	require.Len(t, fixture.routeListener.deleted, 1)
	assert.Equal(t, route1.ID, fixture.routeListener.deleted[0].ID)
	require.Len(t, fixture.apiListener.removed, 1)
	assert.Equal(t, api1.URLPattern, fixture.apiListener.removed[0].URLPattern)
	_, ok = adapter.projectionOwnership.LoadProjection(key2)
	assert.False(t, ok)
}

func TestApplyNacosInstanceProjectionRemovesResourcesMissingFromNewRecord(t *testing.T) {
	fixture := newProjectionManagerFixture(t)
	adapter := newProjectionTestAdapter()
	key := testOwnerKey()
	clusterName, oldEndpoint, oldRoute, oldAPI, oldRecord := testProjection("endpoint-1", "SayHello")
	_, newEndpoint, newRoute, newAPI, newRecord := testProjection("endpoint-2", "Wave")
	require.NoError(t, adapter.ApplyNacosInstanceProjection(key, oldRecord, []*model.Endpoint{oldEndpoint}, []*model.Router{oldRoute}, []router.API{oldAPI}))

	err := adapter.ApplyNacosInstanceProjection(key, newRecord, []*model.Endpoint{newEndpoint}, []*model.Router{newRoute}, []router.API{newAPI})

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{newEndpoint.ID}, endpointIDsForCluster(t, fixture.clusterManager, clusterName))
	require.Len(t, fixture.routeListener.deleted, 1)
	assert.Equal(t, oldRoute.ID, fixture.routeListener.deleted[0].ID)
	require.Len(t, fixture.apiListener.removed, 1)
	assert.Equal(t, oldAPI.URLPattern, fixture.apiListener.removed[0].URLPattern)
	require.Len(t, fixture.routeListener.added, 2)
	assert.Equal(t, newRoute.ID, fixture.routeListener.added[1].ID)
	require.Len(t, fixture.apiListener.added, 2)
	assert.Equal(t, newAPI.URLPattern, fixture.apiListener.added[1].URLPattern)
	loaded, ok := adapter.projectionOwnership.LoadProjection(key)
	require.True(t, ok)
	assert.Equal(t, newRecord, loaded)
}

func TestApplyNacosInstanceProjectionWithEmptyRecordDeletesProjection(t *testing.T) {
	fixture := newProjectionManagerFixture(t)
	adapter := newProjectionTestAdapter()
	key := testOwnerKey()
	clusterName, endpoint, route, api, record := testProjection("endpoint-1", "SayHello")
	require.NoError(t, adapter.ApplyNacosInstanceProjection(key, record, []*model.Endpoint{endpoint}, []*model.Router{route}, []router.API{api}))

	err := adapter.ApplyNacosInstanceProjection(key, registrycommon.ProjectionRecord{}, nil, nil, nil)

	require.NoError(t, err)
	assert.Empty(t, endpointIDsForCluster(t, fixture.clusterManager, clusterName))
	require.Len(t, fixture.routeListener.deleted, 1)
	assert.Equal(t, route.ID, fixture.routeListener.deleted[0].ID)
	require.Len(t, fixture.apiListener.removed, 1)
	assert.Equal(t, api.URLPattern, fixture.apiListener.removed[0].URLPattern)
	_, ok := adapter.projectionOwnership.LoadProjection(key)
	assert.False(t, ok)
}

func TestAdapterApplyReturnsRegistryConstructionError(t *testing.T) {
	adapter := &Adapter{
		id:         "dubbo-registry",
		registries: map[string]dubboRegistry.Registry{},
		cfg: &AdaptorConfig{
			Registries: map[string]model.Registry{
				"nacos-main": {
					Protocol:     constant.Nacos,
					RegistryType: "interface",
					Address:      "not-a-host-port",
				},
			},
		},
	}

	err := adapter.Apply()

	require.EqualError(t, err, "registry-type interface is not supported in this release")
}

type projectionManagerFixture struct {
	clusterManager *server.ClusterManager
	apiListener    *capturedAPIConfigListener
	routeListener  *capturedRouterListener
}

func newProjectionManagerFixture(t *testing.T) projectionManagerFixture {
	t.Helper()

	clusterManager := server.CreateDefaultClusterManager(&model.Bootstrap{})
	routerManager := server.CreateDefaultRouterManager(nil, &model.Bootstrap{})
	apiConfigManager := server.CreateDefaultApiConfigManager(nil, &model.Bootstrap{})
	apiListener := &capturedAPIConfigListener{}
	routeListener := &capturedRouterListener{}
	apiConfigManager.AddApiConfigListener("dubbo-registry", apiListener)
	routerManager.AddRouterListener(routeListener)

	originalClusterManager := getClusterManagerForProjection
	originalRouterManager := getRouterManagerForProjection
	originalAPIConfigManager := getAPIConfigManagerForProjection
	t.Cleanup(func() {
		getClusterManagerForProjection = originalClusterManager
		getRouterManagerForProjection = originalRouterManager
		getAPIConfigManagerForProjection = originalAPIConfigManager
	})
	getClusterManagerForProjection = func() *server.ClusterManager {
		return clusterManager
	}
	getRouterManagerForProjection = func() *server.RouterManager {
		return routerManager
	}
	getAPIConfigManagerForProjection = func() *server.ApiConfigManager {
		return apiConfigManager
	}

	return projectionManagerFixture{
		clusterManager: clusterManager,
		apiListener:    apiListener,
		routeListener:  routeListener,
	}
}

func newProjectionTestAdapter() *Adapter {
	return &Adapter{
		id:                  "dubbo-registry",
		projectionOwnership: registrycommon.NewMemoryProjectionOwnership(),
	}
}

func testOwnerKey() registrycommon.OwnerKey {
	return testOwnerKeyForInstance("instance-1")
}

func testOwnerKeyForInstance(instanceID string) registrycommon.OwnerKey {
	return registrycommon.OwnerKey{
		RegistryID:             "nacos-main",
		ApplicationServiceName: "DemoProviderApp",
		InstanceIdentity:       instanceID,
	}
}

func testProjection(endpointID string, methodName string) (string, *model.Endpoint, *model.Router, router.API, registrycommon.ProjectionRecord) {
	clusterName := "DemoProviderApp/org.apache.demo.Greeter//"
	endpoint := &model.Endpoint{
		ID: endpointID,
		Address: model.SocketAddress{
			Address: "127.0.0.1",
			Port:    20880,
		},
		Metadata: map[string]string{
			constant.InterfaceKey:     "org.apache.demo.Greeter",
			"protocol":                "tri",
			constant.SerializationKey: "hessian2",
		},
	}
	path := "/DemoProviderApp/org.apache.demo.Greeter/" + methodName
	route := &model.Router{
		ID: path,
		Match: model.RouterMatch{
			Path:    path,
			Methods: []string{constant.Post},
		},
		Route: model.RouteAction{Cluster: clusterName},
	}
	api := router.API{
		URLPattern: path,
		Method: config.Method{
			Enable:   true,
			HTTPVerb: constant.Post,
			IntegrationRequest: config.IntegrationRequest{
				RequestType: constant.DubboRequest,
				DubboBackendConfig: config.DubboBackendConfig{
					Method:         methodName,
					ParameterTypes: []string{"java.lang.String"},
				},
			},
		},
	}
	record := registrycommon.ProjectionRecord{
		Services: map[string]registrycommon.ServiceProjection{
			clusterName: {
				ClusterName: clusterName,
				EndpointIDs: []string{endpoint.ID},
				RouterIDs:   []string{route.ID},
				APIKeys: []registrycommon.APIKey{
					{Path: api.URLPattern, HTTPMethod: constant.Post},
				},
			},
		},
	}
	return clusterName, endpoint, route, api, record
}

func endpointIDsForCluster(t *testing.T, clusterManager *server.ClusterManager, clusterName string) []string {
	t.Helper()

	store, err := clusterManager.CloneStore()
	require.NoError(t, err)
	for _, cluster := range store.Config {
		if cluster.Name != clusterName {
			continue
		}
		ids := make([]string, 0, len(cluster.Endpoints))
		for _, endpoint := range cluster.Endpoints {
			ids = append(ids, endpoint.ID)
		}
		return ids
	}
	return nil
}
