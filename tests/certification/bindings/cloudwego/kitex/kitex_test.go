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
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cloudwego/kitex-examples/kitex_gen/api"
	"github.com/cloudwego/kitex-examples/kitex_gen/api/echo"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/cloudwego/kitex"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	bindings_loader "github.com/dapr/dapr/pkg/components/bindings"
	"github.com/dapr/dapr/pkg/runtime"
	daprsdk "github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

const (
	hostports   = "127.0.0.1:8888"
	destService = "echo"
	MethodName  = "echo"
	sidecarName = "kitex-sidecar"
	bindingName = "cloudwego-kitex-binding"
)

const (
	metadataRPCMethodName  = "methodName"
	metadataRPCDestService = "destService"
	metadataRPCHostports   = "host-ports"
	metadataRPCVersion     = "version"
)

func TestKitexBinding(t *testing.T) {

	testKitexInvocation := func(ctx flow.Context) error {
		client, clientErr := daprsdk.NewClientWithPort(fmt.Sprint(runtime.DefaultDaprAPIGRPCPort))
		if clientErr != nil {
			panic(clientErr)
		}
		defer client.Close()
		codec := utils.NewThriftMessageCodec()

		req := &api.EchoEchoArgs{Req: &api.Request{Message: "hello dapr"}}

		reqData, err := codec.Encode(MethodName, thrift.CALL, 0, req)
		assert.Nil(t, err)
		metadata := map[string]string{
			metadataRPCVersion:     "4.0.0",
			metadataRPCHostports:   hostports,
			metadataRPCDestService: destService,
			metadataRPCMethodName:  MethodName,
		}

		invokeRequest := &daprsdk.InvokeBindingRequest{
			Name:      bindingName,
			Operation: string(bindings.GetOperation),
			Metadata:  metadata,
			Data:      reqData,
		}

		resp, err := client.InvokeBinding(ctx, invokeRequest)
		assert.Nil(t, err)
		result := &api.EchoEchoResult{}

		_, _, err = codec.Decode(resp.Data, result)
		assert.Nil(t, err)
		assert.Equal(t, "hello dapr,hi Kitex", result.Success.Message)
		time.Sleep(time.Second)
		return nil
	}

	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		assert.Nil(t, runKitexServer(stopCh))
	}()
	time.Sleep(time.Second * 3)

	flow.New(t, "test kitex binding config").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components"),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithBindings(newBindingsRegistry()))).
		Step("verify kitex invocation", testKitexInvocation).
		Run()
}

func newBindingsRegistry() *bindings_loader.Registry {
	log := logger.NewLogger("dapr.components")

	r := bindings_loader.NewRegistry()
	r.Logger = log
	r.RegisterOutputBinding(kitex.NewKitexOutput, "cloudwego.kitex")
	return r
}

func runKitexServer(stop chan struct{}) error {
	svr := echo.NewServer(new(EchoImpl))
	if err := svr.Run(); err != nil {
		klog.Errorf("server stopped with error:", err)
	} else {
		klog.Errorf("server stopped")
	}

	after := time.After(time.Second * 10)
	select {
	case <-stop:
	case <-after:
	}
	return nil
}

var _ api.Echo = &EchoImpl{}

type EchoImpl struct{}

// Echo implements the Echo interface.
func (s *EchoImpl) Echo(ctx context.Context, req *api.Request) (resp *api.Response, err error) {
	klog.Info("echo called")
	return &api.Response{Message: req.Message + ",hi Kitex"}, nil
}
