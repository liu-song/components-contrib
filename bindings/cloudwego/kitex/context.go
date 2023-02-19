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
	"github.com/pkg/errors"
)

const (
	metadataRPCMethodName  = "methodName"
	metadataRPCDestService = "destService"
	metadataRPCHostports   = "host-ports"
	metadataRPCVersion     = "version"
)

type kitexContext struct {
	version     string
	destService string
	hostports   string
	method      string
	inited      bool
	client      genericclient.Client
}

func newKitexContext(metadata map[string]string) *kitexContext {
	kitexMetadata := &kitexContext{}
	kitexMetadata.version = metadata[metadataRPCVersion]
	kitexMetadata.destService = metadata[metadataRPCDestService]
	kitexMetadata.hostports = metadata[metadataRPCHostports]
	kitexMetadata.method = metadata[metadataRPCMethodName]
	kitexMetadata.inited = false
	return kitexMetadata
}

func (d *kitexContext) Init(Metadata map[string]string) error {
	if d.inited {
		return nil
	}
	var destService, hostports string
	destService, ok := Metadata[metadataRPCDestService]
	if !ok {
		return errors.Errorf("metadataRPCDestService isn't exist")
	}
	hostports, ok = Metadata[metadataRPCHostports]
	if !ok {
		return errors.Errorf("metadataRPCHostports isn't exist")
	}
	_, ok = Metadata[metadataRPCMethodName]
	if !ok {
		return errors.Errorf("metadataRPCMethodName isn't exist")
	}

	genericCli, err := genericclient.NewClient(destService, generic.BinaryThriftGeneric(), client.WithHostPorts(hostports))
	if err != nil {
		return errors.Errorf("Get gerneric service of kitex failed")
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
	return fmt.Sprintf("%s.%s.%s.%s", d.version, d.destService, d.method, d.hostports)
}
