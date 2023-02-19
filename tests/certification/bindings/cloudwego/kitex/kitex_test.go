package kitex

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cloudwego/kitex-examples/kitex_gen/api"
	"github.com/cloudwego/kitex-examples/kitex_gen/api/echo"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"log"

	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	hostports   = "127.0.0.1:8888"
	destService = "cloudwego"
	MethodName  = "echo"
)

func TestInvoke(t *testing.T) {
	// 0. init dapr provided and kitex server
	stopCh := make(chan struct{})
	defer close(stopCh)
	go func() {
		assert.Nil(t, runKitexServer(stopCh))
	}()
	time.Sleep(time.Second * 3)

	output := NewKitexOutput(logger.NewLogger("hello kitex"))

	//genericCli, err := genericclient.NewClient(destService, generic.BinaryThriftGeneric(), client.WithHostPorts(hostports))
	//if err != nil {
	//	klog.Fatal(err)
	//}
	codec := utils.NewThriftMessageCodec()

	req := &api.EchoEchoArgs{Req: &api.Request{Message: "my request"}}

	ctx := context.Background() // 注意 destService  和  method 的使用
	buf, err := codec.Encode(MethodName, thrift.CALL, 0, req)
	assert.Nil(t, err)
	//resp, err := genericCli.GenericCall(ctx, MethodName, buf) //二进制，泛化调用

	resp, err := output.Invoke(ctx, &bindings.InvokeRequest{
		Metadata: map[string]string{
			metadataRPCVersion:     "4.0.0",
			metadataRPCHostports:   hostports,
			metadataRPCDestService: destService,
			metadataRPCMethodName:  MethodName,
		},
		Data:      buf,
		Operation: bindings.GetOperation,
	})

	if err != nil {
		klog.Errorf("call echo failed: %w\n", err)
	}
	result := &api.EchoEchoResult{}
	//if v, ok := resp.(*bindings.InvokeResponse); ok {
	//	//跟什么类型判断就只能调用什么类型的方法
	//	v("BrainWu")
	//}

	_, _, err = codec.Decode(resp.Data, result)
	if err != nil {
		klog.Fatal(err)
	}
	klog.Info(result.Success)
	time.Sleep(time.Second)

}

func runKitexServer(stop chan struct{}) error {
	svr := echo.NewServer(new(EchoImpl))
	if err := svr.Run(); err != nil {
		log.Println("server stopped with error:", err)
	} else {
		log.Println("server stopped")
	}

	after := time.After(time.Second * 10)
	select {
	case <-stop:
	case <-after:
	}
	return nil
}

var _ api.Echo = &EchoImpl{}

// EchoImpl implements the last service interface defined in the IDL.
type EchoImpl struct{}

// Echo implements the Echo interface.
func (s *EchoImpl) Echo(ctx context.Context, req *api.Request) (resp *api.Response, err error) {
	klog.Info("echo called")
	return &api.Response{Message: req.Message + "+clouewego"}, nil
}
