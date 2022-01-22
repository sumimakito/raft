package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/sumimakito/raft/pb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

type apiServiceServer struct {
	server *Server
	pb.UnimplementedAPIServiceServer
}

func (s *apiServiceServer) Apply(ctx context.Context, body *pb.LogBody) (*pb.ApplyLogResponse, error) {
	logBody := LogBody{Data: body.Data}
	switch body.Type {
	case pb.LogType_LOG_COMMAND:
		logBody.Type = LogCommand
	case pb.LogType_LOG_CONFIGURATION:
		logBody.Type = LogConfiguration
	}
	result, err := s.server.Apply(ctx, logBody).Result()
	if err != nil {
		return &pb.ApplyLogResponse{Error: err.Error()}, nil
	}
	logMeta := result.(LogMeta)
	return &pb.ApplyLogResponse{Meta: &pb.LogMeta{Index: logMeta.Index, Term: logMeta.Term}}, nil
}

func (s *apiServiceServer) ApplyCommand(ctx context.Context, cmd *pb.Command) (*pb.ApplyLogResponse, error) {
	result, err := s.server.ApplyCommand(ctx, cmd.Data).Result()
	if err != nil {
		return &pb.ApplyLogResponse{Error: err.Error()}, nil
	}
	logMeta := result.(LogMeta)
	return &pb.ApplyLogResponse{Meta: &pb.LogMeta{Index: logMeta.Index, Term: logMeta.Term}}, nil
}

type apiMembersAddRequest struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

type apiErrorResponse struct {
	Error error `json:"error"`
}

type apiServerRouters struct {
	root   *mux.Router
	api    *mux.Router
	apiExt *mux.Router
	apiV1  *mux.Router
}

type APIExtension interface {
	Setup(s *Server, r *mux.Router) error
}

type apiServer struct {
	server *Server

	apiSvcSvr *apiServiceServer

	grpcServer *grpc.Server
	httpServer *http.Server

	routers    apiServerRouters
	extensions []APIExtension
}

func newAPIServer(server *Server, extensions ...APIExtension) *apiServer {
	s := &apiServer{
		server:     server,
		grpcServer: grpc.NewServer(),
		routers:    apiServerRouters{},
		extensions: extensions,
	}
	s.apiSvcSvr = &apiServiceServer{server: server}
	pb.RegisterAPIServiceServer(s.grpcServer, s.apiSvcSvr)

	// Bind HTTP handler with GRPC handler
	httpHandler, grpcHandler := s.setupRouters(), s.grpcServer
	httpGRPCHandler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcHandler.ServeHTTP(rw, r)
			return
		}
		httpHandler.ServeHTTP(rw, r)
	})

	// Configure HTTP server with HTTP/2
	http2Server := &http2.Server{}

	// With TLS ...
	// s.httpServer = &http.Server{Handler: httpGRPCHandler}
	// Must1(http2.ConfigureServer(httpServer, http2Server))

	// Without TLS ...
	s.httpServer = &http.Server{Handler: h2c.NewHandler(httpGRPCHandler, http2Server)}

	return s
}

// setupRouters sets up the routers and returns the root router
func (s *apiServer) setupRouters() *mux.Router {
	s.routers.root = mux.NewRouter()
	s.routers.api = s.routers.root.PathPrefix("/api").Subrouter()
	s.routers.apiExt = s.routers.api.PathPrefix("/extension").Subrouter()
	s.routers.apiV1 = s.routers.api.PathPrefix("/v1").Subrouter()

	s.routers.apiV1.HandleFunc("/configuration", func(rw http.ResponseWriter, r *http.Request) {
		h := NewHandyRespWriter(rw, s.server.logger.Desugar())
		h.JSON(s.server.confStore.Latest())
	}).Methods("GET")

	s.routers.apiV1.HandleFunc("/logs", func(rw http.ResponseWriter, r *http.Request) {
		h := NewHandyRespWriter(rw, s.server.logger.Desugar())
		h.JSONFunc(func() (v interface{}, statusCode int, err error) {
			bodyData, err := ioutil.ReadAll(r.Body)
			if err != nil {
				return nil, 0, err
			}
			result, err := s.server.Apply(r.Context(), LogBody{Type: LogCommand, Data: bodyData}).Result()
			if err != nil {
				return nil, 0, err
			}
			return result.(LogMeta), 0, nil
		})
	}).Methods("POST")

	s.routers.apiV1.HandleFunc("/states", func(rw http.ResponseWriter, r *http.Request) {
		h := NewHandyRespWriter(rw, s.server.logger.Desugar())
		h.JSON(s.server.States())
	}).Methods("GET")

	s.routers.apiV1.HandleFunc("/members", func(rw http.ResponseWriter, r *http.Request) {
		h := NewHandyRespWriter(rw, s.server.logger.Desugar())
		h.JSON(s.server.confStore.Latest().Peers())
	}).Methods("GET")

	s.routers.apiV1.HandleFunc("/members", func(rw http.ResponseWriter, r *http.Request) {
		h := NewHandyRespWriter(rw, s.server.logger.Desugar())
		h.JSONFunc(func() (v interface{}, statusCode int, err error) {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				return nil, 0, err
			}
			var apiRequest apiMembersAddRequest
			if err := json.Unmarshal(body, &apiRequest); err != nil {
				return nil, 0, err
			}
			if err := s.server.Register(Peer{
				ID:       ServerID(apiRequest.ID),
				Endpoint: ServerEndpoint(apiRequest.Endpoint),
			}); err != nil {
				return apiErrorResponse{Error: err}, http.StatusBadRequest, nil
			}
			return nil, http.StatusNoContent, nil
		})
	}).Methods("POST")

	s.routers.apiV1.HandleFunc("/snapshot", func(rw http.ResponseWriter, r *http.Request) {
		h := NewHandyRespWriter(rw, s.server.logger.Desugar())
		if err := s.server.takeSnapshot(); err != nil {
			h.Error(err)
			return
		}
		h.WriteHeader(http.StatusNoContent)
	}).Methods("GET")

	for _, extension := range s.extensions {
		Must1(extension.Setup(s.server, s.routers.apiExt))
	}

	return s.routers.root
}

func (s *apiServer) Serve(listener net.Listener) error {
	s.server.logger.Infow("API server started",
		logFields(s.server,
			"address", listener.Addr(),
			"endpoint", fmt.Sprintf("http://%s", listener.Addr()))...)
	return s.httpServer.Serve(listener)
}
