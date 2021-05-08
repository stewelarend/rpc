package http

import (
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"path"
	"strings"

	"github.com/stewelarend/rpc"
	"github.com/stewelarend/util"
)

func init() {
	rpc.RegisterServer(
		"http",
		&config{
			Address: "",
			Port:    8000,
		},
	)
}

type config struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

func (c *config) Validate() error {
	if c.Port == 0 {
		c.Port = 8000
	}
	if c.Port < 0 {
		return fmt.Errorf("negative port:%d", c.Port)
	}
	return nil
}

func (c config) Create(service rpc.IService, tmplRequest rpc.IRequest, tmplResponse rpc.IResponse) (rpc.IServer, error) {
	fmt.Printf("Create HTTP server: %+v\n", c)
	return httpRpcServer{
		service:      service,
		config:       c,
		tmplRequest:  tmplRequest,
		tmplResponse: tmplResponse,
	}, nil
}

type httpRpcServer struct {
	service      rpc.IService
	config       config
	tmplRequest  rpc.IRequest
	tmplResponse rpc.IResponse
}

func (s httpRpcServer) Run() error {
	return nethttp.ListenAndServe(fmt.Sprintf("%s:%d", s.config.Address, s.config.Port), s)
}

func (s httpRpcServer) ServeHTTP(res nethttp.ResponseWriter, req *nethttp.Request) {
	fmt.Printf("HTTP %s %s\n", req.Method, req.URL.Path)
	if req.Method != nethttp.MethodGet && req.Method != nethttp.MethodPost {
		nethttp.Error(res, "method must be GET or POST", nethttp.StatusMethodNotAllowed)
		return
	}

	operName := path.Clean(req.URL.Path)[1:] //skip leading '/'
	operHandler, ok := s.service.Oper(operName)
	if !ok {
		nethttp.Error(
			res,
			fmt.Sprintf("operation \"%s\" does not exist, only %s",
				operName,
				strings.Join(s.service.Opers(), "|")),
			nethttp.StatusNotFound)
		return
	}

	//parse body only if POST
	if req.Method == nethttp.MethodPost {
		newOperHandler, err := util.StructFromJSONReader(operHandler, req.Body)
		if err != nil {
			nethttp.Error(
				res,
				fmt.Sprintf(
					"invalid body: %v",
					err),
				nethttp.StatusBadRequest)
			return
		}
		operHandler = newOperHandler.(rpc.IHandler)
	}

	//parse URL params into the struct
	{
		newOperHandler, err := util.StructFromMap(operHandler, urlParams(req.URL.Query()))
		if err != nil {
			nethttp.Error(
				res,
				fmt.Sprintf(
					"invalid URL params: %v",
					err),
				nethttp.StatusBadRequest)
			return
		}
		operHandler = newOperHandler.(rpc.IHandler)
	}

	//validate if possible
	if validator, ok := operHandler.(rpc.IValidator); ok {
		if err := validator.Validate(); err != nil {
			nethttp.Error(
				res,
				fmt.Sprintf(
					"invalid request: %v",
					err),
				nethttp.StatusBadRequest)
			return
		}
	}

	operRes, err := operHandler.Exec(nil)
	if err != nil {
		nethttp.Error(
			res,
			fmt.Sprintf("operation \"%s\" failed: %v",
				operName,
				err),
			nethttp.StatusBadRequest) //todo - wrong error!
		return
	}
	jsonRes, err := json.Marshal(operRes)
	if err != nil {
		nethttp.Error(
			res,
			fmt.Sprintf("operation \"%s\" response encoding failed: %v",
				operName,
				err),
			nethttp.StatusBadRequest) //todo - wrong error!
		return
	}

	res.Header().Set("Content-Type", "application/json")
	res.Write(jsonRes)
}

func urlParams(p map[string][]string) map[string]interface{} {
	paramsObject := map[string]interface{}{}
	for n, v := range p {
		if len(v) > 1 {
			paramsObject[n] = v
		} else {
			paramsObject[n] = v[0]
		}
	}
	return paramsObject
}
