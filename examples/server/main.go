package main

import (
	"github.com/wuqtao/regdis"
	"log"
	"time"
)

func main() {
	service := regdis.NewService("server", "rpcServer", "server1", "tcp", "127.0.0.1", 8080)
	serviceReg, err := regdis.NewEtcdRegistrationDiscovery([]string{"127.0.0.1:2379"}, "", "", "/service/rpc", false, time.Second*5)
	if err != nil {
		log.Fatalf("register grpc service fail:%s\n", err.Error())
	}
	err = serviceReg.Register(service)
	if err != nil {
		log.Fatalf("register grpc service fail:%s\n", err.Error())
	}
	time.Sleep(time.Minute * 5)

}
