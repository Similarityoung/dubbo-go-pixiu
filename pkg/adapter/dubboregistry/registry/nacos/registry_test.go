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
	"testing"
)

import (
	"github.com/creasty/defaults"

	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

import (
	pixiuModel "github.com/apache/dubbo-go-pixiu/pkg/model"
)

type mockNamingClient struct {
	naming_client.INamingClient

	subscribedServices []string
}

func (m *mockNamingClient) Subscribe(param *vo.SubscribeParam) error {
	m.subscribedServices = append(m.subscribedServices, param.ServiceName)
	return nil
}

func TestParseNacosRegistryMode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    nacosRegistryMode
		wantErr string
	}{
		{
			name:  "omitted uses service discovery",
			input: "",
			want:  modeService,
		},
		{
			name:  "service uses service discovery",
			input: "service",
			want:  modeService,
		},
		{
			name:  "all uses service discovery",
			input: "all",
			want:  modeService,
		},
		{
			name:    "interface is unsupported",
			input:   "interface",
			wantErr: "registry-type interface is not supported in this release",
		},
		{
			name:    "application is not an alias",
			input:   "application",
			wantErr: "registry-type application is not valid; use service",
		},
		{
			name:    "unknown value fails",
			input:   "bogus",
			wantErr: "unknown registry-type \"bogus\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNacosRegistryMode(tt.input)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewNacosRegistryRejectsUnsupportedModeBeforeAddressParsing(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "interface",
			input:   "interface",
			wantErr: "registry-type interface is not supported in this release",
		},
		{
			name:    "application",
			input:   "application",
			wantErr: "registry-type application is not valid; use service",
		},
		{
			name:    "unknown",
			input:   "bogus",
			wantErr: "unknown registry-type \"bogus\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newNacosRegistry(pixiuModel.Registry{
				RegistryType: tt.input,
				Address:      "not-a-host-port",
			}, nil)
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func TestRegistryTypeDefaultDoesNotMaskOmittedNacosMode(t *testing.T) {
	regConfig := pixiuModel.Registry{}

	require.NoError(t, defaults.Set(&regConfig))

	assert.Empty(t, regConfig.RegistryType)
}

func TestNacosAppListenerSkipsInterfaceRegistryServiceNames(t *testing.T) {
	client := &mockNamingClient{}
	listener := newNacosAppListener(client, nil, &pixiuModel.Registry{
		Group: "DEFAULT_GROUP",
	}, nil).(*nacosAppListener)

	err := listener.updateServiceList([]string{
		"DemoProviderApp",
		"providers:org.apache.demo.Greeter:1.0.0:group",
		"AnotherProviderApp",
	})

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"DemoProviderApp", "AnotherProviderApp"}, client.subscribedServices)
	assert.NotContains(t, client.subscribedServices, "providers:org.apache.demo.Greeter:1.0.0:group")
	assert.NotContains(t, listener.appInfoMap, "providers:org.apache.demo.Greeter:1.0.0:group")
}
