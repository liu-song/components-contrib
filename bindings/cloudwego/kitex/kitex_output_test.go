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
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cloudwego/kitex"
	"github.com/cloudwego/kitex-examples/kitex_gen/api"
	"github.com/cloudwego/kitex-examples/kitex_gen/api/echo"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	hostports   = "127.0.0.1:8888"
	destService = "echo"
	MethodName  = "echo"
)

func TestInvoke(t *testing.T) {
	// 0. init  Kitex server
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		assert.Nil(t, runKitexServer(stopCh))
	}()
	time.Sleep(time.Second * 3)

	// 1 create KitexOutput
	output := NewKitexOutput(logger.NewLogger("hello dapr && CloudWeGo"))

	// 2 create req bytes
	codec := utils.NewThriftMessageCodec()
	req := &api.EchoEchoArgs{Req: &api.Request{Message: "hello dapr"}}
	ctx := context.Background()
	reqData, err := codec.Encode(MethodName, thrift.CALL, 0, req)
	assert.Nil(t, err)

	// 3. invoke dapr kitex output binding, get rsp bytes
	resp, err := output.Invoke(ctx, &bindings.InvokeRequest{
		Metadata: map[string]string{
			metadataRPCVersion:     kitex.Version,
			metadataRPCHostports:   hostports,
			metadataRPCDestService: destService,
			metadataRPCMethodName:  MethodName,
		},
		Data:      reqData,
		Operation: bindings.GetOperation,
	})
	assert.Nil(t, err)

	// 4. get rep value
	result := &api.EchoEchoResult{}
	_, _, err = codec.Decode(resp.Data, result)
	assert.Nil(t, err)
	assert.Equal(t, "hello dapr,hi Kitex", result.Success.Message)
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
