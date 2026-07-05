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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryProjectionOwnershipStoresByOwnerKey(t *testing.T) {
	ownership := NewMemoryProjectionOwnership()
	key := OwnerKey{
		RegistryID:             "nacos-main",
		ApplicationServiceName: "DemoProviderApp",
		InstanceIdentity:       "instance-1",
	}
	record := ProjectionRecord{
		Services: map[string]ServiceProjection{
			"DemoProviderApp/org.apache.demo.Greeter//": {
				ClusterName: "DemoProviderApp/org.apache.demo.Greeter//",
				EndpointIDs: []string{"endpoint-1"},
				RouterIDs:   []string{"/DemoProviderApp/org.apache.demo.Greeter/SayHello"},
				APIKeys: []APIKey{
					{Path: "/DemoProviderApp/org.apache.demo.Greeter/SayHello", HTTPMethod: "POST"},
				},
			},
		},
	}

	ownership.StoreProjection(key, record)
	record.Services["DemoProviderApp/org.apache.demo.Greeter//"] = ServiceProjection{}

	loaded, ok := ownership.LoadProjection(key)
	require.True(t, ok)
	assert.Equal(t, "DemoProviderApp/org.apache.demo.Greeter//", loaded.Services["DemoProviderApp/org.apache.demo.Greeter//"].ClusterName)
	assert.Equal(t, []string{"endpoint-1"}, loaded.Services["DemoProviderApp/org.apache.demo.Greeter//"].EndpointIDs)

	loaded.Services["DemoProviderApp/org.apache.demo.Greeter//"] = ServiceProjection{}
	loadedAgain, ok := ownership.LoadProjection(key)
	require.True(t, ok)
	assert.Equal(t, []string{"endpoint-1"}, loadedAgain.Services["DemoProviderApp/org.apache.demo.Greeter//"].EndpointIDs)

	visited := 0
	ownership.ForEachProjection(func(gotKey OwnerKey, gotRecord ProjectionRecord) bool {
		visited++
		assert.Equal(t, key, gotKey)
		assert.Equal(t, []string{"endpoint-1"}, gotRecord.Services["DemoProviderApp/org.apache.demo.Greeter//"].EndpointIDs)
		gotRecord.Services["DemoProviderApp/org.apache.demo.Greeter//"] = ServiceProjection{}
		return true
	})
	assert.Equal(t, 1, visited)
	loadedAfterVisit, ok := ownership.LoadProjection(key)
	require.True(t, ok)
	assert.Equal(t, []string{"endpoint-1"}, loadedAfterVisit.Services["DemoProviderApp/org.apache.demo.Greeter//"].EndpointIDs)

	ownership.DeleteProjection(key)
	_, ok = ownership.LoadProjection(key)
	assert.False(t, ok)
}
