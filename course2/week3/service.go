package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"log"

	"net"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type aclData map[string][]string

func newAclDataFromJSON(in string) (*aclData, error) {
	res := &aclData{}
	if err := json.Unmarshal([]byte(in), res); err != nil {
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

func NewAdminService() *AdminService {
	return &AdminService{}
}

func (as *AdminService) Logging(in *Nothing, s Admin_LoggingServer) error {
	return nil
}

func (as *AdminService) Statistics(in *StatInterval, s Admin_StatisticsServer) error {
	return nil
}

func (s *MyService) start() error {
	log.Printf("Starting MyService at %s", s.listenAddr)
	lis, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	RegisterAdminServer(server, NewAdminService())

	go func() {
		select {
		case <-s.ctx.Done():
			server.Stop()
			if err := lis.Close(); err != nil {
				log.Printf("error while closing listener: %v", err)
			}
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
