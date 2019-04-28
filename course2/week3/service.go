package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"regexp"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type aclData struct {
	data map[string][]string
	sr   *regexp.Regexp
}

func (a *aclData) checkACL(consumer, method string) bool {
	allowedMethods, ok := a.data[consumer]

	mn := "aclData.checkACL"
	log.Printf("[%s] consumer: %s, method: %s", mn, consumer, method)

	if !ok {
		return false
	}

	parsedMethod := a.sr.Split(method, -1)

	log.Printf("[%s] parsedMethod: %v", mn, parsedMethod)

	if len(parsedMethod) != 3 {
		log.Printf("[%s] parsedMethod len not 3, return false", mn)
		return false
	}

	for _, m := range allowedMethods {
		pm := a.sr.Split(m, -1)
		log.Printf("[%s] Process: %v", mn, pm)
		if pm[1] == parsedMethod[1] && (pm[2] == "*" || pm[2] == parsedMethod[2]) {
			log.Printf("[%s] Return true", mn)
			return true
		}
	}

	return false
}

func newAclDataFromJSON(in string) (*aclData, error) {
	res := &aclData{
		data: make(map[string][]string),
		sr:   regexp.MustCompile(`/`),
	}
	if err := json.Unmarshal([]byte(in), &res.data); err != nil {
		return nil, err
	}
	return res, nil
}

type MyService struct {
	ctx        context.Context
	listenAddr string
	aclData    *aclData
}

type AdminService struct{}

func NewAdminService() AdminServer {
	return &AdminService{}
}

func (as *AdminService) Logging(in *Nothing, s Admin_LoggingServer) error {
	return nil
}

func (as *AdminService) Statistics(in *StatInterval, s Admin_StatisticsServer) error {
	return nil
}

type BizService struct{}

func NewBizService() BizServer {
	return &BizService{}
}

func (bs *BizService) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bs *BizService) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bs *BizService) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (s *MyService) checkACL(consumer, method string) bool {
	return s.aclData.checkACL(consumer, method)
}

func (s *MyService) getUnaryACLInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		log.Printf("aclInterceptor call")
		md, ok := metadata.FromIncomingContext(ctx)

		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "unable to parse metadata from request context")
		}

		cons := md.Get("consumer")

		log.Printf("[%s] consumers: %#v", "aclInterceptor", cons)

		if len(cons) < 1 {
			return nil, status.Errorf(codes.Unauthenticated, "consumer is not provided")
		}

		log.Printf("[%s] full method: %#v", "aclInterceptor", info.FullMethod)

		if !s.checkACL(cons[0], info.FullMethod) {
			return nil, status.Errorf(codes.Unauthenticated, "method not allowed")
		}

		return handler(ctx, req)
	}
}

type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	return w.ServerStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	return w.ServerStream.SendMsg(m)
}

func newWrappedStream(s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{s}
}

func (s *MyService) getStreamACLInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())

		if !ok {
			return status.Errorf(codes.Unauthenticated, "unable to parse metadata from request context")
		}

		cons := md.Get("consumer")

		log.Printf("[%s] consumers: %#v", "aclInterceptor", cons)

		if len(cons) < 1 {
			return status.Errorf(codes.Unauthenticated, "consumer is not provided")
		}

		log.Printf("[%s] full method: %#v", "aclInterceptor", info.FullMethod)

		if !s.checkACL(cons[0], info.FullMethod) {
			return status.Errorf(codes.Unauthenticated, "method not allowed")
		}

		return handler(srv, newWrappedStream(ss))
	}
}

func (s *MyService) start() error {
	log.Printf("Starting MyService at %s", s.listenAddr)

	server := grpc.NewServer(
		grpc.UnaryInterceptor(s.getUnaryACLInterceptor()),
		grpc.StreamInterceptor(s.getStreamACLInterceptor()),
	)
	RegisterAdminServer(server, NewAdminService())
	RegisterBizServer(server, NewBizService())

	lis, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := server.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	go func() {
		select {
		case <-s.ctx.Done():
			server.Stop()
			return
		}
	}()

	return nil
}

func StartMyMicroservice(ctx context.Context, listenAddr string, aclData string) error {
	acl, err := newAclDataFromJSON(aclData)
	if err != nil {
		return err
	}
	ms := &MyService{ctx, listenAddr, acl}
	return ms.start()
}
