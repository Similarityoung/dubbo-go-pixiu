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
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	dubboCommon "dubbo.apache.org/dubbo-go/v3/common"
	dubboConst "dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/metadata/info"
	dr "dubbo.apache.org/dubbo-go/v3/registry"
	"dubbo.apache.org/dubbo-go/v3/registry/servicediscovery"
	"dubbo.apache.org/dubbo-go/v3/remoting"

	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	nacosModel "github.com/nacos-group/nacos-sdk-go/model"
)

import (
	common2 "github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/common"
	"github.com/apache/dubbo-go-pixiu/pkg/adapter/dubboregistry/registry"
	"github.com/apache/dubbo-go-pixiu/pkg/common/constant"
	"github.com/apache/dubbo-go-pixiu/pkg/config"
	"github.com/apache/dubbo-go-pixiu/pkg/logger"
	"github.com/apache/dubbo-go-pixiu/pkg/model"
	"github.com/apache/dubbo-go-pixiu/pkg/router"
)

var _ registry.Listener = new(appServiceListener)

type appServiceListener struct {
	client      naming_client.INamingClient
	instanceMap map[string]nacosModel.Instance
	cacheLock   sync.Mutex

	exit            chan struct{}
	wg              sync.WaitGroup
	adapterListener common2.RegistryEventListener
	registryID      string
	applicationName string
}

type MethodSignature struct {
	MethodName     string
	ParameterTypes []string
}

type instanceProjection struct {
	key       common2.OwnerKey
	record    common2.ProjectionRecord
	endpoints []*model.Endpoint
	routers   []*model.Router
	apis      []router.API
}

type nacosInstanceProjectionApplier interface {
	ApplyNacosInstanceProjection(common2.OwnerKey, common2.ProjectionRecord, []*model.Endpoint, []*model.Router, []router.API) error
}

func newNacosAppSrvListener(client naming_client.INamingClient, adapterListener common2.RegistryEventListener, registryID string, applicationName string) *appServiceListener {
	return &appServiceListener{
		client:          client,
		exit:            make(chan struct{}),
		adapterListener: adapterListener,
		instanceMap:     map[string]nacosModel.Instance{},
		registryID:      registryID,
		applicationName: applicationName,
	}
}

func (l *appServiceListener) WatchAndHandle() {
	panic("implement me")
}

func (l *appServiceListener) Close() {
	close(l.exit)
	l.wg.Wait()
}

func (l *appServiceListener) Callback(services []nacosModel.SubscribeService, err error) {
	if err != nil {
		logger.Errorf("nacos subscribe callback error:%s", err.Error())
		return
	}

	addInstances := make([]nacosModel.Instance, 0, len(services))
	delInstances := make([]nacosModel.Instance, 0, len(services))
	updateInstances := make([]nacosModel.Instance, 0, len(services))
	newInstanceMap := make(map[string]nacosModel.Instance, len(services))

	l.cacheLock.Lock()
	defer l.cacheLock.Unlock()
	for i := range services {
		if !services[i].Enable || !services[i].Healthy {
			// instance is not available, so ignore it
			continue
		}
		host := services[i].Ip + ":" + strconv.Itoa(int(services[i].Port))
		services[i].ServiceName = handleServiceName(services[i].ServiceName)
		instance := generateInstance(services[i])
		newInstanceMap[host] = instance
		if old, ok := l.instanceMap[host]; !ok {
			// instance does not exist in cache, add it
			addInstances = append(addInstances, instance)
		} else {
			if !reflect.DeepEqual(old, instance) {
				// instance is different from cache, update it
				updateInstances = append(updateInstances, instance)
			}
		}
	}

	for host, inst := range l.instanceMap {
		if _, ok := newInstanceMap[host]; !ok {
			// cache instance does not exist in new instance list, remove it from cache
			delInstances = append(delInstances, inst)
		}
	}

	l.instanceMap = newInstanceMap
	for i := range addInstances {
		l.handleInstance(addInstances[i], remoting.EventTypeAdd)
	}
	for i := range delInstances {
		l.handleInstance(delInstances[i], remoting.EventTypeDel)
	}
	for i := range updateInstances {
		l.handleInstance(updateInstances[i], remoting.EventTypeUpdate)
	}
}

func (l *appServiceListener) handleInstance(instance nacosModel.Instance, action remoting.EventType) {
	if action == remoting.EventTypeDel {
		l.deleteInstanceProjection(instance)
		return
	}

	projection, err := l.buildInstanceProjection(instance)
	if err != nil {
		logger.Errorf("build nacos dubbo projection failed: %s", err.Error())
		return
	}
	if projection == nil || len(projection.apis) == 0 {
		l.deleteInstanceProjection(instance)
		return
	}

	applier, ok := l.adapterListener.(nacosInstanceProjectionApplier)
	if !ok {
		logger.Warnf("nacos dubbo projection listener is not available")
		return
	}
	if err := applier.ApplyNacosInstanceProjection(projection.key, projection.record, projection.endpoints, projection.routers, projection.apis); err != nil {
		logger.Errorf("apply nacos dubbo projection failed: %s", err.Error())
	}
}

func (l *appServiceListener) deleteInstanceProjection(instance nacosModel.Instance) {
	if projectionListener, ok := l.adapterListener.(common2.ProjectionEventListener); ok {
		if err := projectionListener.DeleteInstanceProjection(l.ownerKey(instance)); err != nil {
			logger.Errorf("delete nacos dubbo projection failed: %s", err.Error())
		}
	}
}

func (l *appServiceListener) buildInstanceProjection(nmis nacosModel.Instance) (*instanceProjection, error) {
	instance := toNacosInstance(nmis)
	metadata := instance.GetMetadata()
	metadataInfo, err := servicediscovery.GetMetadataInfo(instance.GetServiceName(), instance, metadata[dubboConst.ExportedServicesRevisionPropertyName])
	if err != nil {
		return nil, err
	}
	if metadataInfo == nil || len(metadataInfo.Services) == 0 {
		return nil, nil
	}
	instance.SetServiceMetadata(metadataInfo)

	projection := &instanceProjection{
		key: l.ownerKey(nmis),
		record: common2.ProjectionRecord{
			Services: make(map[string]common2.ServiceProjection),
		},
	}
	for _, service := range metadataInfo.Services {
		l.appendServiceProjection(projection, instance, service)
	}
	if len(projection.record.Services) == 0 {
		return nil, nil
	}
	return projection, nil
}

func (l *appServiceListener) appendServiceProjection(projection *instanceProjection, instance dr.ServiceInstance, service *info.ServiceInfo) {
	signatures := methodSignatures(service)
	if len(signatures) == 0 {
		return
	}

	uniqueMethods := uniqueMethodSignatures(signatures)
	if len(uniqueMethods) == 0 {
		return
	}

	urls := instance.ToURLs(service)
	for _, url := range urls {
		if url == nil {
			continue
		}
		serviceID := serviceIdentityFromURL(l.applicationName, url)
		if !serviceID.valid() {
			continue
		}
		endpoint, ok := endpointFromURL(serviceID, url)
		if !ok {
			continue
		}

		serviceProjection := projection.record.Services[serviceID.canonical()]
		serviceProjection.ClusterName = serviceID.canonical()
		serviceProjection.EndpointIDs = append(serviceProjection.EndpointIDs, endpoint.ID)
		projection.endpoints = append(projection.endpoints, endpoint)

		if len(serviceProjection.APIKeys) == 0 {
			for methodName, parameterTypes := range uniqueMethods {
				api, route := generatedAPIAndRouter(serviceID, methodName, parameterTypes)
				serviceProjection.RouterIDs = append(serviceProjection.RouterIDs, route.ID)
				serviceProjection.APIKeys = append(serviceProjection.APIKeys, common2.APIKey{
					Path:       api.URLPattern,
					HTTPMethod: constant.Post,
				})
				projection.routers = append(projection.routers, route)
				projection.apis = append(projection.apis, api)
			}
		}
		projection.record.Services[serviceID.canonical()] = serviceProjection
	}
}

func (l *appServiceListener) ownerKey(instance nacosModel.Instance) common2.OwnerKey {
	return common2.OwnerKey{
		RegistryID:             l.registryID,
		ApplicationServiceName: l.applicationName,
		InstanceIdentity:       instanceIdentity(instance),
	}
}

func instanceIdentity(instance nacosModel.Instance) string {
	if strings.TrimSpace(instance.InstanceId) != "" {
		return strings.TrimSpace(instance.InstanceId)
	}
	return instance.Ip + ":" + strconv.Itoa(int(instance.Port))
}

type dubboServiceIdentity struct {
	application string
	service     string
	version     string
	group       string
}

func serviceIdentityFromURL(application string, url *dubboCommon.URL) dubboServiceIdentity {
	return dubboServiceIdentity{
		application: strings.TrimSpace(application),
		service:     strings.TrimSpace(url.Interface()),
		version:     strings.TrimSpace(url.Version()),
		group:       strings.TrimSpace(url.Group()),
	}
}

func (s dubboServiceIdentity) valid() bool {
	return s.application != "" && s.service != ""
}

func (s dubboServiceIdentity) canonical() string {
	return strings.Join([]string{s.application, s.service, s.version, s.group}, constant.PathSlash)
}

func endpointFromURL(serviceID dubboServiceIdentity, url *dubboCommon.URL) (*model.Endpoint, bool) {
	protocol := strings.TrimSpace(url.Protocol)
	serialization := strings.TrimSpace(url.GetParam(constant.SerializationKey, ""))
	if serviceID.service == "" || protocol == "" || serialization == "" {
		return nil, false
	}

	host, port, ok := splitEndpointAddress(url.Location)
	if !ok {
		return nil, false
	}
	endpointID := strings.Join([]string{serviceID.canonical(), url.Location, protocol, serialization}, "|")
	metadata := map[string]string{
		constant.ApplicationKey:   serviceID.application,
		constant.InterfaceKey:     serviceID.service,
		constant.VersionKey:       serviceID.version,
		constant.GroupKey:         serviceID.group,
		"protocol":                protocol,
		constant.SerializationKey: serialization,
	}
	if timeout := strings.TrimSpace(url.GetParam(dubboConst.TimeoutKey, "")); timeout != "" {
		metadata[dubboConst.TimeoutKey] = timeout
	}
	return &model.Endpoint{
		ID: endpointID,
		Address: model.SocketAddress{
			Address: host,
			Port:    port,
		},
		Metadata: metadata,
	}, true
}

func splitEndpointAddress(location string) (string, int, bool) {
	host, portString, err := net.SplitHostPort(location)
	if err != nil {
		idx := strings.LastIndex(location, ":")
		if idx <= 0 || idx == len(location)-1 {
			return "", 0, false
		}
		host = location[:idx]
		portString = location[idx+1:]
	}
	port, err := strconv.Atoi(portString)
	if err != nil || strings.TrimSpace(host) == "" || port <= 0 {
		return "", 0, false
	}
	return host, port, true
}

func generatedAPIAndRouter(serviceID dubboServiceIdentity, methodName string, parameterTypes []string) (router.API, *model.Router) {
	path := strings.Join([]string{"", serviceID.application, serviceID.service, methodName}, constant.PathSlash)
	api := router.API{
		URLPattern: path,
		Method: config.Method{
			Enable:   true,
			Timeout:  3 * time.Second,
			Mock:     false,
			HTTPVerb: constant.Post,
			InboundRequest: config.InboundRequest{
				RequestType: constant.HTTPRequest,
			},
			IntegrationRequest: config.IntegrationRequest{
				RequestType: constant.DubboRequest,
				DubboBackendConfig: config.DubboBackendConfig{
					Method:         methodName,
					ParameterTypes: append([]string(nil), parameterTypes...),
				},
				MappingParams: []config.MappingParam{
					{
						Name:  "requestBody.values",
						MapTo: "opt.values",
					},
				},
			},
		},
	}
	route := &model.Router{
		ID: path,
		Match: model.RouterMatch{
			Path:    path,
			Methods: []string{constant.Post},
		},
		Route: model.RouteAction{
			Cluster: serviceID.canonical(),
		},
	}
	return api, route
}

func methodSignatures(service *info.ServiceInfo) []MethodSignature {
	if service == nil || len(service.Params) == 0 {
		return nil
	}
	methods := splitMetadataList(service.Params[constant.MethodsKey])
	if len(methods) == 0 {
		return nil
	}
	signatures := make([]MethodSignature, 0, len(methods))
	for _, methodName := range methods {
		parameterTypes, ok := parameterTypesForMethod(service.Params, methodName)
		if !ok {
			continue
		}
		signatures = append(signatures, MethodSignature{
			MethodName:     methodName,
			ParameterTypes: parameterTypes,
		})
	}
	return signatures
}

func uniqueMethodSignatures(signatures []MethodSignature) map[string][]string {
	grouped := make(map[string][][]string, len(signatures))
	for _, signature := range signatures {
		methodName := strings.TrimSpace(signature.MethodName)
		if methodName == "" {
			continue
		}
		grouped[methodName] = append(grouped[methodName], append([]string(nil), signature.ParameterTypes...))
	}

	unique := make(map[string][]string, len(grouped))
	for methodName, parameterTypes := range grouped {
		if len(parameterTypes) == 1 {
			unique[methodName] = parameterTypes[0]
		}
	}
	return unique
}

func parameterTypesForMethod(params map[string]string, methodName string) ([]string, bool) {
	keys := []string{
		"methods." + methodName + ".parameterTypes",
		methodName + ".parameterTypes",
	}
	for _, key := range keys {
		raw, ok := params[key]
		if !ok {
			continue
		}
		return parseParameterTypes(raw)
	}
	return nil, false
}

func parseParameterTypes(raw string) ([]string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	var jsonTypes []string
	if err := json.Unmarshal([]byte(raw), &jsonTypes); err == nil {
		return cleanParameterTypes(jsonTypes)
	}
	return cleanParameterTypes(splitMetadataList(raw))
}

func splitMetadataList(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func cleanParameterTypes(types []string) ([]string, bool) {
	cleaned := make([]string, len(types))
	for i, item := range types {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, false
		}
		cleaned[i] = item
	}
	return cleaned, true
}

func (l *appServiceListener) handle(url *dubboCommon.URL, action remoting.EventType) {
	logger.Infof("update begin, service event : %v %v", action, url)

	// NOTE: _ is methods, we can not get methods by application discovery
	bkConfig, _, location, err := registry.ParseDubboString(url.String())
	if err != nil {
		logger.Errorf("parse dubbo url error = %s", err)
		return
	}

	apiPattern := registry.GetAPIPattern(bkConfig)
	mappingParams := []config.MappingParam{
		{
			Name:  "requestBody.values",
			MapTo: "opt.values",
		},
		{
			Name:  "requestBody.types",
			MapTo: "opt.types",
		},
	}

	api := registry.CreateAPIConfig(apiPattern, location, bkConfig, constant.AnyValue, mappingParams)
	if action == remoting.EventTypeDel {
		if err := l.adapterListener.OnRemoveAPI(api); err != nil {
			logger.Errorf("Error={%s} happens when try to remove api %s", err.Error(), api.Path)
			return
		}
	} else {
		if err := l.adapterListener.OnAddAPI(api); err != nil {
			logger.Errorf("Error={%s} happens when try to add api %s", err.Error(), api.Path)
			return
		}
	}
}

func (l *appServiceListener) getURLs(nmis nacosModel.Instance) []*dubboCommon.URL {
	instance := toNacosInstance(nmis)
	metadata := instance.GetMetadata()
	metadataInfo, err := servicediscovery.GetMetadataInfo(instance.GetServiceName(), instance, metadata[dubboConst.ExportedServicesRevisionPropertyName])
	if err != nil {
		logger.Errorf("get instance metadata info error %v", err.Error())
		return nil
	}
	instance.SetServiceMetadata(metadataInfo)
	urls := make([]*dubboCommon.URL, 0, len(metadataInfo.Services))
	for _, service := range metadataInfo.Services {
		urls = append(urls, instance.ToURLs(service)...)
	}
	return urls
}

// toNacosInstance convert to registry's service instance
func toNacosInstance(nmis nacosModel.Instance) dr.ServiceInstance {
	md := make(map[string]string, len(nmis.Metadata))
	for k, v := range nmis.Metadata {
		md[k] = fmt.Sprint(v)
	}
	return &dr.DefaultServiceInstance{
		ID:          nmis.InstanceId,
		ServiceName: nmis.ServiceName,
		Host:        nmis.Ip,
		Port:        int(nmis.Port),
		Enable:      nmis.Enable,
		Healthy:     nmis.Healthy,
		Metadata:    md,
	}
}

// group@@serviceName convert to serviceName
func handleServiceName(serviceName string) string {
	parts := strings.Split(serviceName, "@@")
	if len(parts) > 1 {
		return parts[1]
	}
	return serviceName
}
