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
	"strconv"
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
	logDataChan  []*chan *Event
}

func (s *MyService) getLogChannel() *chan *Event {
	s.logDataMutex.Lock()
	defer s.logDataMutex.Unlock()
	ch := make(chan *Event)
	s.logDataChan = append(s.logDataChan, &ch)
	return &ch
}

func (s *MyService) closeChannel(ch *chan *Event) {
	s.logDataMutex.Lock()
	defer s.logDataMutex.Unlock()
	for i, och := range s.logDataChan {
		if och == ch {
			s.logDataChan = append(s.logDataChan[:i], s.logDataChan[i+1:]...)
			close(*och)
			return
		}
	}
}

func (s *MyService) addLogData(timestamp int64, consumer, method, host string) {
	s.logDataMutex.RLock()
	defer s.logDataMutex.RUnlock()
	if len(s.logDataChan) > 0 {
		e := &Event{
			Timestamp: timestamp,
			Consumer:  consumer,
			Method:    method,
			Host:      host,
		}
		log.Printf("[MyService.addLogData] add event: %#v", e)
		for _, ch := range s.logDataChan {
			*ch <- e
		}
	}
}

func (e *Event) getHash() string {
	return strconv.FormatInt(e.Timestamp, 16) + e.Consumer + e.Method + e.Host
}

func (s *MyService) getStat(since int64, to int64) *Stat {
	res := &Stat{
		Timestamp:  time.Now().Unix(),
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}
	s.logDataMutex.RLock()
	defer s.logDataMutex.RUnlock()

	set := make(map[string]struct{})

	for _, e := range s.logData {
		eHash := e.getHash()
		if _, ok := set[eHash]; e.Timestamp < since || e.Timestamp > to || ok {
			continue
		}
		//if e.Timestamp < since || e.Timestamp > to {
		//	continue
		//}
		set[eHash] = struct{}{}
		if curCount, ok := res.ByMethod[e.Method]; ok {
			res.ByMethod[e.Method] = curCount + 1
		} else {
			res.ByMethod[e.Method] = 1
		}
		if curCount, ok := res.ByConsumer[e.Consumer]; ok {
			res.ByConsumer[e.Consumer] = curCount + 1
		} else {
			res.ByConsumer[e.Consumer] = 1
		}
	}
	log.Printf("[MyService.getStat] since %d to %d, res: %v", since, to, res)
	return res
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

	logCh := as.s.getLogChannel()
	defer as.s.closeChannel(logCh)

	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-*logCh:
			if err := s.Send(e); err != nil {
				log.Printf("(%d) [AdminService.Logging] error while sending log event: %s", funcId, err)
			}
		}
	}
}

func (as *AdminService) Statistics(in *StatInterval, s Admin_StatisticsServer) error {
	funcId := rand.Uint64()
	ctx := s.Context()

	stat := &Stat{
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}

	logCh := as.s.getLogChannel()
	defer as.s.closeChannel(logCh)

	ticker := time.NewTicker(time.Second * time.Duration(in.IntervalSeconds))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			stat.Timestamp = t.Unix()
			if err := s.Send(stat); err != nil {
				log.Printf("(%d) [AdminService.Statistics] error while sending statistics: %s", funcId, err)
			}
			stat = &Stat{
				ByMethod:   make(map[string]uint64),
				ByConsumer: make(map[string]uint64),
			}
		case e := <-*logCh:
			if curCount, ok := stat.ByMethod[e.Method]; ok {
				stat.ByMethod[e.Method] = curCount + 1
			} else {
				stat.ByMethod[e.Method] = 1
			}
			if curCount, ok := stat.ByConsumer[e.Consumer]; ok {
				stat.ByConsumer[e.Consumer] = curCount + 1
			} else {
				stat.ByConsumer[e.Consumer] = 1
			}
		}
	}
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
	ms := &MyService{ctx, listenAddr, acl, make([]*Event, 0), &sync.RWMutex{}, make([]*chan *Event, 0)}
	return ms.start()
}
