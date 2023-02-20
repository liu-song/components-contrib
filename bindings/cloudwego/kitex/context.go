/*
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kitex

import (
	"context"
	"fmt"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/client/genericclient"
	"github.com/cloudwego/kitex/pkg/generic"
	"golang.org/x/xerrors"
)

const (
	metadataRPCMethodName  = "methodName"
	metadataRPCDestService = "destService"
	metadataRPCHostports   = "hostPorts"
	metadataRPCVersion     = "version"
)

type kitexContext struct {
	version     string
	destService string
	hostPorts   string
	method      string
	inited      bool
	client      genericclient.Client
}

func newKitexContext(metadata map[string]string) *kitexContext {
	kitexMetadata := &kitexContext{}
	kitexMetadata.version = metadata[metadataRPCVersion]
	kitexMetadata.destService = metadata[metadataRPCDestService]
	kitexMetadata.hostPorts = metadata[metadataRPCHostports]
	kitexMetadata.method = metadata[metadataRPCMethodName]
	kitexMetadata.inited = false
	return kitexMetadata
}

func (d *kitexContext) Init(metadata map[string]string) error {
	if d.inited {
		return nil
	}
	var destService, hostPorts string
	destService, ok := metadata[metadataRPCDestService]
	if !ok {
		return xerrors.Errorf("metadataRPCDestService isn't exist")
	}
	hostPorts, ok = metadata[metadataRPCHostports]
	if !ok {
		return xerrors.Errorf("metadataRPCHostPorts isn't exist")
	}
	_, ok = metadata[metadataRPCMethodName]
	if !ok {
		return xerrors.Errorf("metadataRPCMethodName isn't exist")
	}

	genericCli, err := genericclient.NewClient(destService, generic.BinaryThriftGeneric(), client.WithHostPorts(hostPorts))
	if err != nil {
		return xerrors.Errorf("Get gerneric service of kitex failed")
	}
	d.client = genericCli
	d.inited = true
	return nil
}

func (d *kitexContext) Invoke(ctx context.Context, body []byte) (interface{}, error) {
	resp, err := d.client.GenericCall(ctx, d.method, body)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (d *kitexContext) String() string {
	return fmt.Sprintf("%s.%s.%s.%s", d.version, d.destService, d.method, d.hostPorts)
}
