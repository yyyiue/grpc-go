/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package roundrobin_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/grpclog/glogger"
	_ "google.golang.org/grpc/health" // 执行 client.go 里的 init
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/internal/leakcheck"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	testpb "google.golang.org/grpc/test/grpc_testing"
)

type testServer struct {
	healthpb.UnimplementedHealthServer
	testpb.UnimplementedTestServiceServer
}

func (s *testServer) EmptyCall(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
	return &testpb.Empty{}, nil
}

func (s *testServer) FullDuplexCall(stream testpb.TestService_FullDuplexCallServer) error {
	return nil
}

func (s *testServer) Check(ctx context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}, nil
}

func (s *testServer) Watch(in *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	var done bool
	for {
		select {
		// Server 关闭
		case <-stream.Context().Done():
			done = true
		default:
			//case <-s.update:
		}
		if done {
			break
		}
		//s.mu.Lock()
		//resp := &healthpb.HealthCheckResponse{
		//	Status: s.status[in.Service],
		//}
		//s.mu.Unlock()

		resp := &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}
		stream.SendMsg(resp)
		// 每3秒发一次健康状况
		time.Sleep(3 * time.Second)
	}

	return nil
}

type test struct {
	servers   []*grpc.Server
	addresses []string
}

func (t *test) cleanup() {
	for _, s := range t.servers {
		s.Stop()
	}
}

func startTestServers(count int) (_ *test, err error) {
	t := &test{}

	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		testpb.RegisterTestServiceServer(s, &testServer{})
		healthpb.RegisterHealthServer(s, &testServer{})
		t.servers = append(t.servers, s)
		t.addresses = append(t.addresses, lis.Addr().String())

		go func(s *grpc.Server, l net.Listener) {
			s.Serve(l)
		}(s, lis)
	}

	return t, nil
}

func startTestServersAddr(addr string) (_ *test, err error) {
	t := &test{}

	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen %v", err)
	}

	s := grpc.NewServer()
	testpb.RegisterTestServiceServer(s, &testServer{})
	t.servers = append(t.servers, s)
	t.addresses = append(t.addresses, lis.Addr().String())

	go func(s *grpc.Server, l net.Listener) {
		s.Serve(l)
	}(s, lis)

	return t, nil
}

func Tes1tOneBackend(t *testing.T) {
	defer leakcheck.Check(t)
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	testc := testpb.NewTestServiceClient(cc)
	// The first RPC should fail because there's no address.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
	}

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}}})
	// The second RPC should succeed.
	if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
	}
}

func Tes1tBackendsRoundRobin(t *testing.T) {
	defer leakcheck.Check(t)
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	backendCount := 5
	test, err := startTestServers(backendCount)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	testc := testpb.NewTestServiceClient(cc)
	// The first RPC should fail because there's no address.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
	}

	var resolvedAddrs []resolver.Address
	for i := 0; i < backendCount; i++ {
		resolvedAddrs = append(resolvedAddrs, resolver.Address{Addr: test.addresses[i]})
	}

	// 更新 resolver 状态，可能会重新生成 AC
	//time.Sleep(10 * time.Second)
	r.UpdateState(resolver.State{Addresses: resolvedAddrs})
	var p peer.Peer
	// Make sure connections to all servers are up.
	for si := 0; si < backendCount; si++ {
		var connected bool
		for i := 0; i < 1000; i++ {
			// 获取 server 端地址
			if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
				t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
			}
			if p.Addr.String() == test.addresses[si] {
				connected = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !connected {
			t.Fatalf("Connection to %v was not up after more than 1 second", test.addresses[si])
		}
	}

	// 3轮请求
	for i := 0; i < 3*backendCount; i++ {
		if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
		if p.Addr.String() != test.addresses[i%backendCount] {
			t.Fatalf("Index %d: want peer %v, got peer %v", i, test.addresses[i%backendCount], p.Addr.String())
		}
	}
}

func Tes1tAddressesRemoved(t *testing.T) {
	defer leakcheck.Check(t)
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	testc := testpb.NewTestServiceClient(cc)
	// The first RPC should fail because there's no address.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
	}

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}}})
	// The second RPC should succeed.
	if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
	}

	r.UpdateState(resolver.State{Addresses: []resolver.Address{}})
	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := testc.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); status.Code(err) == codes.DeadlineExceeded {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("No RPC failed after removing all addresses, want RPC to fail with DeadlineExceeded")
}

func Tes1tCloseWithPendingRPC(t *testing.T) {
	defer leakcheck.Check(t)
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	testc := testpb.NewTestServiceClient(cc)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This RPC blocks until cc is closed.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) == codes.DeadlineExceeded {
				t.Errorf("RPC failed because of deadline after cc is closed; want error the client connection is closing")
			}
			cancel()
		}()
	}
	cc.Close()
	wg.Wait()
}

func Tes1tNewAddressWhileBlocking(t *testing.T) {
	defer leakcheck.Check(t)
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	test, err := startTestServers(1)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	testc := testpb.NewTestServiceClient(cc)
	// The first RPC should fail because there's no address.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
	}

	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}}})
	// The second RPC should succeed.
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() = _, %v, want _, nil", err)
	}

	r.UpdateState(resolver.State{Addresses: []resolver.Address{}})

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This RPC blocks until NewAddress is called.
			testc.EmptyCall(context.Background(), &testpb.Empty{})
		}()
	}
	time.Sleep(50 * time.Millisecond)
	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: test.addresses[0]}}})
	wg.Wait()
}

func TestOneServerDown(t *testing.T) {
	defer leakcheck.Check(t)
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	backendCount := 3
	test, err := startTestServers(backendCount)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	serviceConfig := `{
    "loadBalancingPolicy": "round_robin",
    "healthCheckConfig": {"serviceName": "grpc.testing.TestService"}
}`
	//cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithDefaultServiceConfig(serviceConfig))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	testc := testpb.NewTestServiceClient(cc)
	// The first RPC should fail because there's no address.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
	}

	var resolvedAddrs []resolver.Address
	for i := 0; i < backendCount; i++ {
		resolvedAddrs = append(resolvedAddrs, resolver.Address{Addr: test.addresses[i]})
	}

	r.UpdateState(resolver.State{Addresses: resolvedAddrs})
	var p peer.Peer
	// Make sure connections to all servers are up.
	for si := 0; si < backendCount; si++ {
		var connected bool
		for i := 0; i < 1000; i++ {
			if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
				t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
			}
			if p.Addr.String() == test.addresses[si] {
				connected = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !connected {
			t.Fatalf("Connection to %v was not up after more than 1 second", test.addresses[si])
		}
	}

	for i := 0; i < 3*backendCount; i++ {
		if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
		if p.Addr.String() != test.addresses[i%backendCount] {
			t.Fatalf("Index %d: want peer %v, got peer %v", i, test.addresses[i%backendCount], p.Addr.String())
		}
	}

	// Stop one server, RPCs should roundrobin among the remaining servers.
	backendCount--
	test.servers[backendCount].Stop()

	fmt.Println("结束一个")
	// 观察连接重连情况
	//time.Sleep(100 * time.Second)

	fmt.Println("开始调用")
	// Loop until see server[backendCount-1] twice without seeing server[backendCount].
	var targetSeen int
	for i := 0; i < 1000; i++ {
		if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
			targetSeen = 0
			t.Logf("EmptyCall() = _, %v, want _, <nil>", err)
			// Due to a race, this RPC could possibly get the connection that
			// was closing, and this RPC may fail. Keep trying when this
			// happens.
			continue
		}
		switch p.Addr.String() {
		case test.addresses[backendCount-1]:
			// 非最后一个被剔除的 server
			targetSeen++
		case test.addresses[backendCount]:
			// Reset targetSeen if peer is server[backendCount].
			targetSeen = 0
		}
		// Break to make sure the last picked address is server[-1], so the following for loop won't be flaky.
		// round_robin 两轮，下次请求从第一个 server 开始
		if targetSeen >= 2 {
			fmt.Println("break")
			break
		}
	}
	if targetSeen != 2 {
		t.Fatal("Failed to see server[backendCount-1] twice without seeing server[backendCount]")
	}

	fmt.Println("这里")
	for i := 0; i < 3*backendCount; i++ {
		if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
		if p.Addr.String() != test.addresses[i%backendCount] {
			t.Errorf("Index %d: want peer %v, got peer %v", i, test.addresses[i%backendCount], p.Addr.String())
		}
	}
	fmt.Println("这里!!")

	test.servers[0].Stop()
	test.servers[1].Stop()
	test.servers[1].Stop()
}

func Tes1tAllServersDown(t *testing.T) {
	defer leakcheck.Check(t)
	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	backendCount := 3
	test, err := startTestServers(backendCount)
	if err != nil {
		t.Fatalf("failed to start servers: %v", err)
	}
	defer test.cleanup()

	cc, err := grpc.Dial(r.Scheme()+":///test.server", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	testc := testpb.NewTestServiceClient(cc)
	// The first RPC should fail because there's no address.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := testc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
	}

	var resolvedAddrs []resolver.Address
	for i := 0; i < backendCount; i++ {
		resolvedAddrs = append(resolvedAddrs, resolver.Address{Addr: test.addresses[i]})
	}

	r.UpdateState(resolver.State{Addresses: resolvedAddrs})
	var p peer.Peer
	// Make sure connections to all servers are up.
	for si := 0; si < backendCount; si++ {
		var connected bool
		for i := 0; i < 1000; i++ {
			if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
				t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
			}
			if p.Addr.String() == test.addresses[si] {
				connected = true
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !connected {
			t.Fatalf("Connection to %v was not up after more than 1 second", test.addresses[si])
		}
	}

	for i := 0; i < 3*backendCount; i++ {
		if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}, grpc.Peer(&p)); err != nil {
			t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
		}
		if p.Addr.String() != test.addresses[i%backendCount] {
			t.Fatalf("Index %d: want peer %v, got peer %v", i, test.addresses[i%backendCount], p.Addr.String())
		}
	}

	// All servers are stopped, failfast RPC should fail with unavailable.
	for i := 0; i < backendCount; i++ {
		test.servers[i].Stop()
	}
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 1000; i++ {
		if _, err := testc.EmptyCall(context.Background(), &testpb.Empty{}); status.Code(err) == codes.Unavailable {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Failfast RPCs didn't fail with Unavailable after all servers are stopped")
}
