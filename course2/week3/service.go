package main

import (
	"context"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log"
	"math/rand"
	"net"
	"regexp"
	"sync"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type aclData struct {
	data map[string][]string
	sr   *regexp.Regexp
}

func (a *aclData) checkACL(consumer, method string) bool {
	allowedMethods, ok := a.data[consumer]

	if !ok {
		return false
	}

	parsedMethod := a.sr.Split(method, -1)

	if len(parsedMethod) != 3 {
		return false
	}

	for _, m := range allowedMethods {
		pm := a.sr.Split(m, -1)
		if pm[1] == parsedMethod[1] && (pm[2] == "*" || pm[2] == parsedMethod[2]) {
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
	ctx          context.Context
	listenAddr   string
	aclData      *aclData
	logData      []*Event
	logDataMutex *sync.RWMutex
	//statData      []*Stat
	//statDataMutex *sync.RWMutex
}

func (s *MyService) addLogData(timestamp int64, consumer, method, host string) {
	e := &Event{
		Timestamp: timestamp,
		Consumer:  consumer,
		Method:    method,
		Host:      host,
	}
	log.Printf("[MyService.addLogData] add event: %#v", e)
	s.logDataMutex.Lock()
	defer s.logDataMutex.Unlock()
	s.logData = append(s.logData, e)
}

type AdminService struct {
	s *MyService
}

func NewAdminService(s *MyService) AdminServer {
	return &AdminService{s: s}
}

func (as *AdminService) Logging(in *Nothing, s Admin_LoggingServer) error {
	funcId := rand.Uint64()
	ctx := s.Context()
	as.s.logDataMutex.RLock()
	prevLen := len(as.s.logData)
	as.s.logDataMutex.RUnlock()
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case <-ctx.Done():
			return nil
		default:
			as.s.logDataMutex.RLock()
			log.Printf("(%d) [AdminService.Logging] prevLen = %d, cur len = %d", funcId, prevLen, len(as.s.logData))
			if prevLen == len(as.s.logData) {
				continue
			}
			for i := prevLen; i < len(as.s.logData); i++ {
				log.Printf("(%d) [AdminService.Logging] send log event #%d", funcId, i)
				e := as.s.logData[i]

				err := s.Send(e)
				if err != nil {
					log.Printf("(%d) [AdminService.Logging] error while sending log event: %s", funcId, err)
				}
			}
			prevLen = len(as.s.logData)
			as.s.logDataMutex.RUnlock()
		}
	}
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

func getConsumerFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)

	if !ok {
		return "", errors.New("unable to parse metadata from request context")
	}

	cons := md.Get("consumer")

	if len(cons) < 1 {
		return "", errors.New("consumer is not provided")
	}

	return cons[0], nil
}

func getClientHostFromContext(ctx context.Context) string {
	res := ""

	p, ok := peer.FromContext(ctx)

	if ok {
		res = p.Addr.String()
	}

	return res
}

func (s *MyService) getUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		consumer, err := getConsumerFromContext(ctx)

		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, err.Error())
		}

		if !s.checkACL(consumer, info.FullMethod) {
			return nil, status.Errorf(codes.Unauthenticated, "method not allowed")
		}

		s.addLogData(time.Now().Unix(), consumer, info.FullMethod, getClientHostFromContext(ctx))

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
		consumer, err := getConsumerFromContext(ss.Context())

		if err != nil {
			return status.Errorf(codes.Unauthenticated, err.Error())
		}

		if !s.checkACL(consumer, info.FullMethod) {
			return status.Errorf(codes.Unauthenticated, "method not allowed")
		}

		s.addLogData(time.Now().Unix(), consumer, info.FullMethod, getClientHostFromContext(ss.Context()))

		return handler(srv, newWrappedStream(ss))
	}
}

func (s *MyService) start() error {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(s.getUnaryInterceptor()),
		grpc.StreamInterceptor(s.getStreamACLInterceptor()),
	)
	RegisterAdminServer(server, NewAdminService(s))
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
	ms := &MyService{ctx, listenAddr, acl, make([]*Event, 0), &sync.RWMutex{}}
	return ms.start()
}
