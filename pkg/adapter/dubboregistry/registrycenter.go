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
	"os"
	"strconv"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	registrycommon "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry"
	_ "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry/nacos"
	_ "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry/zookeeper"
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/common/extension/adapter"
	"github.com/apache/dubbo-go-pixiu/pkg/config"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	"github.com/apache/dubbo-go-pixiu/pkg/router"
	"github.com/apache/dubbo-go-pixiu/pkg/server"
)

func init() {
	adapter.RegisterAdapterPlugin(&Plugin{})
}

var (
	_ adapter.AdapterPlugin = new(Plugin)
	_ adapter.Adapter       = new(Adapter)
)

var (
	getClusterManagerForProjection   = server.GetClusterManager
	getRouterManagerForProjection    = server.GetRouterManager
	getAPIConfigManagerForProjection = server.GetApiConfigManager
)

type (
	// Plugin to monitor dubbo services on registry center
	Plugin struct {
	}

	AdaptorConfig struct {
		Registries map[string]model.Registry `yaml:"registries" json:"registries" mapstructure:"registries"`
	}
)

// Kind returns the identifier of the plugin
func (p Plugin) Kind() string {
	return constant.DubboRegistryCenterAdapter
}

// CreateAdapter returns the dubbo registry center adapter
func (p *Plugin) CreateAdapter(a *model.Adapter) (adapter.Adapter, error) {
	adapter := &Adapter{id: a.ID,
		registries:          make(map[string]registry.Registry),
		projectionOwnership: registrycommon.NewMemoryProjectionOwnership(),
		cfg:                 &AdaptorConfig{Registries: make(map[string]model.Registry)}}
	return adapter, nil
}

// Adapter to monitor dubbo services on registry center
type Adapter struct {
	id                  string
	cfg                 *AdaptorConfig
	registries          map[string]registry.Registry
	projectionOwnership registrycommon.ProjectionOwnership
}

// Start starts the adaptor
func (a *Adapter) Start() {
	for _, reg := range a.registries {
		if err := reg.Subscribe(); err != nil {
			logger.Errorf("Subscribe fail, error is {%s}", err.Error())
		}
	}
}

// Stop stops the adaptor
func (a *Adapter) Stop() {
	for _, reg := range a.registries {
		if err := reg.Unsubscribe(); err != nil {
			logger.Errorf("Unsubscribe fail, error is {%s}", err.Error())
		}
	}
}

// Apply inits the registries according to the configuration
func (a *Adapter) Apply() error {
	// create registry per config
	nacosAddrFromEnv := os.Getenv(constant.EnvDubbogoPixiuNacosRegistryAddress)
	for k, registryConfig := range a.cfg.Registries {
		var err error
		registryConfig.ID = k
		if nacosAddrFromEnv != "" && registryConfig.Protocol == constant.Nacos {
			registryConfig.Address = nacosAddrFromEnv
		}
		a.registries[k], err = registry.GetRegistry(k, registryConfig, a)
		if err != nil {
			return err
		}
	}

	return nil
}

// Config returns the config of the adaptor
func (a *Adapter) Config() any {
	return a.cfg
}

func (a *Adapter) OnAddAPI(r router.API) error {
	ipPort := strings.Split(r.URL, ":")
	port, err := strconv.Atoi(ipPort[1])
	if err != nil {
		return err
	}
	cluster := getClusterName(r)
	server.GetClusterManager().SetEndpoint(cluster, &model.Endpoint{
		ID: r.URL,
		Address: model.SocketAddress{
			Address: ipPort[0],
			Port:    port,
		}},
	)

	var match model.RouterMatch
	var path string
	if r.DubboBackendConfig.Method == constant.AnyValue {
		path = strings.Join([]string{r.ApplicationName, r.Interface}, constant.PathSlash)
		match = model.RouterMatch{Prefix: path, Methods: []string{string(r.HTTPVerb)}}
	} else {
		path = strings.Join([]string{r.ApplicationName, r.Interface, r.Method.Method}, constant.PathSlash)
		match = model.RouterMatch{Path: path, Methods: []string{string(r.HTTPVerb)}}
	}
	route := model.RouteAction{Cluster: cluster}
	added := &model.Router{ID: path, Match: match, Route: route}
	server.GetRouterManager().AddRouter(added)
	return server.GetApiConfigManager().AddAPI(a.id, r)
}

func (a *Adapter) OnRemoveAPI(r router.API) error {
	cluster := getClusterName(r)
	server.GetClusterManager().DeleteEndpoint(cluster, r.URL)
	return server.GetApiConfigManager().RemoveAPI(a.id, r)
}

func (a *Adapter) OnDeleteRouter(r config.Resource) error {
	acm := server.GetApiConfigManager()
	return acm.DeleteRouter(a.id, r)
}

func (a *Adapter) ApplyInstanceProjection(key registrycommon.OwnerKey, record registrycommon.ProjectionRecord) error {
	a.ensureProjectionOwnership()
	a.projectionOwnership.StoreProjection(key, record)
	return nil
}

func (a *Adapter) DeleteInstanceProjection(key registrycommon.OwnerKey) error {
	a.ensureProjectionOwnership()
	record, ok := a.projectionOwnership.LoadProjection(key)
	if !ok {
		return nil
	}
	if err := a.deleteProjectionResources(key, record, projectionKeepSet{}); err != nil {
		return err
	}
	a.projectionOwnership.DeleteProjection(key)
	return nil
}

func (a *Adapter) ApplyNacosInstanceProjection(
	key registrycommon.OwnerKey,
	record registrycommon.ProjectionRecord,
	endpoints []*model.Endpoint,
	routers []*model.Router,
	apis []router.API,
) error {
	a.ensureProjectionOwnership()
	if len(record.Services) == 0 {
		return a.DeleteInstanceProjection(key)
	}
	oldRecord, hasOldRecord := a.projectionOwnership.LoadProjection(key)
	oldKeep := newProjectionKeepSet(oldRecord)
	clusterByEndpointID := make(map[string]string)
	for _, service := range record.Services {
		for _, endpointID := range service.EndpointIDs {
			clusterByEndpointID[endpointID] = service.ClusterName
		}
	}

	clusterManager := getClusterManagerForProjection()
	if clusterManager == nil {
		return errors.New("cluster manager is not initialized")
	}
	for _, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		clusterName := clusterByEndpointID[endpoint.ID]
		if clusterName == "" {
			return errors.Errorf("projection endpoint %s has no owning cluster", endpoint.ID)
		}
		clusterManager.SetEndpoint(clusterName, endpoint)
	}

	routerManager := getRouterManagerForProjection()
	if routerManager == nil {
		return errors.New("router manager is not initialized")
	}
	for _, route := range routers {
		if route == nil {
			continue
		}
		if oldKeep.keepsRouter(route.ID) || a.routerReferencedByOtherProjection(key, route.ID) {
			continue
		}
		routerManager.AddRouter(route)
	}

	apiConfigManager := getAPIConfigManagerForProjection()
	if apiConfigManager == nil {
		return errors.New("api config manager is not initialized")
	}
	for _, api := range apis {
		apiKey := projectionAPIKey(api)
		if oldKeep.keepsAPI(apiKey) || a.apiReferencedByOtherProjection(key, apiKey) {
			continue
		}
		if err := apiConfigManager.AddAPI(a.id, api); err != nil {
			return err
		}
	}

	if hasOldRecord {
		if err := a.deleteProjectionResources(key, oldRecord, newProjectionKeepSet(record)); err != nil {
			return err
		}
	}

	return a.ApplyInstanceProjection(key, record)
}

func (a *Adapter) ensureProjectionOwnership() {
	if a.projectionOwnership == nil {
		a.projectionOwnership = registrycommon.NewMemoryProjectionOwnership()
	}
}

type projectionEndpointKey struct {
	clusterName string
	endpointID  string
}

type projectionKeepSet struct {
	endpoints map[projectionEndpointKey]struct{}
	routers   map[string]struct{}
	apis      map[registrycommon.APIKey]struct{}
}

func newProjectionKeepSet(record registrycommon.ProjectionRecord) projectionKeepSet {
	keep := projectionKeepSet{
		endpoints: make(map[projectionEndpointKey]struct{}),
		routers:   make(map[string]struct{}),
		apis:      make(map[registrycommon.APIKey]struct{}),
	}
	for _, service := range record.Services {
		for _, endpointID := range service.EndpointIDs {
			keep.endpoints[projectionEndpointKey{
				clusterName: service.ClusterName,
				endpointID:  endpointID,
			}] = struct{}{}
		}
		for _, routerID := range service.RouterIDs {
			keep.routers[routerID] = struct{}{}
		}
		for _, apiKey := range service.APIKeys {
			keep.apis[apiKey] = struct{}{}
		}
	}
	return keep
}

func (k projectionKeepSet) keepsEndpoint(clusterName string, endpointID string) bool {
	if k.endpoints == nil {
		return false
	}
	_, ok := k.endpoints[projectionEndpointKey{
		clusterName: clusterName,
		endpointID:  endpointID,
	}]
	return ok
}

func (k projectionKeepSet) keepsRouter(routerID string) bool {
	if k.routers == nil {
		return false
	}
	_, ok := k.routers[routerID]
	return ok
}

func (k projectionKeepSet) keepsAPI(apiKey registrycommon.APIKey) bool {
	if k.apis == nil {
		return false
	}
	_, ok := k.apis[apiKey]
	return ok
}

func (a *Adapter) routerReferencedByOtherProjection(owner registrycommon.OwnerKey, routerID string) bool {
	found := false
	a.projectionOwnership.ForEachProjection(func(key registrycommon.OwnerKey, record registrycommon.ProjectionRecord) bool {
		if key == owner {
			return true
		}
		if projectionRecordReferencesRouter(record, routerID) {
			found = true
			return false
		}
		return true
	})
	return found
}

func (a *Adapter) apiReferencedByOtherProjection(owner registrycommon.OwnerKey, apiKey registrycommon.APIKey) bool {
	found := false
	a.projectionOwnership.ForEachProjection(func(key registrycommon.OwnerKey, record registrycommon.ProjectionRecord) bool {
		if key == owner {
			return true
		}
		if projectionRecordReferencesAPI(record, apiKey) {
			found = true
			return false
		}
		return true
	})
	return found
}

func projectionRecordReferencesRouter(record registrycommon.ProjectionRecord, routerID string) bool {
	for _, service := range record.Services {
		for _, candidate := range service.RouterIDs {
			if candidate == routerID {
				return true
			}
		}
	}
	return false
}

func projectionRecordReferencesAPI(record registrycommon.ProjectionRecord, apiKey registrycommon.APIKey) bool {
	for _, service := range record.Services {
		for _, candidate := range service.APIKeys {
			if candidate == apiKey {
				return true
			}
		}
	}
	return false
}

func projectionAPIKey(api router.API) registrycommon.APIKey {
	return registrycommon.APIKey{
		Path:       api.URLPattern,
		HTTPMethod: api.HTTPVerb,
	}
}

func (a *Adapter) deleteProjectionResources(owner registrycommon.OwnerKey, record registrycommon.ProjectionRecord, keep projectionKeepSet) error {
	var clusterManager *server.ClusterManager
	var routerManager *server.RouterManager
	var apiConfigManager *server.ApiConfigManager

	for _, service := range record.Services {
		for _, endpointID := range service.EndpointIDs {
			if keep.keepsEndpoint(service.ClusterName, endpointID) {
				continue
			}
			if clusterManager == nil {
				clusterManager = getClusterManagerForProjection()
				if clusterManager == nil {
					return errors.New("cluster manager is not initialized")
				}
			}
			clusterManager.DeleteEndpoint(service.ClusterName, endpointID)
		}
		for _, routerID := range service.RouterIDs {
			if keep.keepsRouter(routerID) {
				continue
			}
			if a.routerReferencedByOtherProjection(owner, routerID) {
				continue
			}
			if routerManager == nil {
				routerManager = getRouterManagerForProjection()
				if routerManager == nil {
					return errors.New("router manager is not initialized")
				}
			}
			routerManager.DeleteRouter(&model.Router{ID: routerID})
		}
		for _, apiKey := range service.APIKeys {
			if keep.keepsAPI(apiKey) {
				continue
			}
			if a.apiReferencedByOtherProjection(owner, apiKey) {
				continue
			}
			if apiConfigManager == nil {
				apiConfigManager = getAPIConfigManagerForProjection()
				if apiConfigManager == nil {
					return errors.New("api config manager is not initialized")
				}
			}
			err := apiConfigManager.RemoveAPI(a.id, router.API{
				URLPattern: apiKey.Path,
				Method: config.Method{
					HTTPVerb: apiKey.HTTPMethod,
				},
			})
			if err != nil && err.Error() != "no listener found" {
				return err
			}
		}
	}
	return nil
}

func getClusterName(r router.API) string {
	return strings.Join([]string{r.ApplicationName, r.Interface, r.Method.Method, r.Version, r.Group}, constant.PathSlash)
}
