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

package common

import (
	"sync"
)

// This file defines the ownership ledger for resources projected from Dubbo
// registry instances.
//
// A registry instance can be translated into Pixiu runtime resources such as
// endpoints, routers, and API configs. Projection ownership records which
// resources were produced by each instance, so the adapter can update or delete
// exactly those resources when the registry instance changes or disappears.

type OwnerKey struct {
	RegistryID             string
	ApplicationServiceName string
	InstanceIdentity       string
}

// APIKey identifies an API config generated from a registry projection.
type APIKey struct {
	Path       string
	HTTPMethod string
}

// ServiceProjection records the runtime resources generated for one Dubbo
// service identity.
type ServiceProjection struct {
	ClusterName string
	EndpointIDs []string
	RouterIDs   []string
	APIKeys     []APIKey
}

// ProjectionRecord is the ownership ledger for all service projections produced
// by a single registry instance.
type ProjectionRecord struct {
	// key = canonical service identity string
	Services map[string]ServiceProjection
}

// ProjectionOwnership stores the mapping from a registry instance owner to the
// runtime resources projected from it.
type ProjectionOwnership interface {
	StoreProjection(key OwnerKey, record ProjectionRecord)
	LoadProjection(key OwnerKey) (ProjectionRecord, bool)
	DeleteProjection(key OwnerKey)
	ForEachProjection(func(OwnerKey, ProjectionRecord) bool)
}

// ProjectionEventListener applies or removes projected runtime resources.
type ProjectionEventListener interface {
	ApplyInstanceProjection(key OwnerKey, record ProjectionRecord) error
	DeleteInstanceProjection(key OwnerKey) error
}

// MemoryProjectionOwnership keeps projection ownership in memory.
type MemoryProjectionOwnership struct {
	mu      sync.RWMutex
	records map[OwnerKey]ProjectionRecord
}

// NewMemoryProjectionOwnership creates an in-memory projection ownership store.
func NewMemoryProjectionOwnership() *MemoryProjectionOwnership {
	return &MemoryProjectionOwnership{
		records: make(map[OwnerKey]ProjectionRecord),
	}
}

// StoreProjection records the current projection for an owner.
func (o *MemoryProjectionOwnership) StoreProjection(key OwnerKey, record ProjectionRecord) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.records[key] = cloneProjectionRecord(record)
}

// LoadProjection returns a copy of the projection stored for an owner.
func (o *MemoryProjectionOwnership) LoadProjection(key OwnerKey) (ProjectionRecord, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	record, ok := o.records[key]
	if !ok {
		return ProjectionRecord{}, false
	}
	return cloneProjectionRecord(record), true
}

// DeleteProjection removes the projection record for an owner.
func (o *MemoryProjectionOwnership) DeleteProjection(key OwnerKey) {
	o.mu.Lock()
	defer o.mu.Unlock()

	delete(o.records, key)
}

// ForEachProjection visits a snapshot of all projection records.
func (o *MemoryProjectionOwnership) ForEachProjection(fn func(OwnerKey, ProjectionRecord) bool) {
	if fn == nil {
		return
	}

	o.mu.RLock()
	records := make(map[OwnerKey]ProjectionRecord, len(o.records))
	for key, record := range o.records {
		records[key] = cloneProjectionRecord(record)
	}
	o.mu.RUnlock()

	for key, record := range records {
		if !fn(key, record) {
			return
		}
	}
}

func cloneProjectionRecord(record ProjectionRecord) ProjectionRecord {
	if record.Services == nil {
		return ProjectionRecord{}
	}
	cloned := ProjectionRecord{
		Services: make(map[string]ServiceProjection, len(record.Services)),
	}
	for key, service := range record.Services {
		service.EndpointIDs = append([]string(nil), service.EndpointIDs...)
		service.RouterIDs = append([]string(nil), service.RouterIDs...)
		service.APIKeys = append([]APIKey(nil), service.APIKeys...)
		cloned.Services[key] = service
	}
	return cloned
}
