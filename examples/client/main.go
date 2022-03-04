package main

import (
	"fmt"
	"github.com/wuqtao/regdis"
	"log"
	"time"
)

func main() {
	serviceReg, err := regdis.NewEtcdRegistrationDiscovery([]string{"127.0.0.1:2379"}, "", "", "/service/rpc", false, time.Second*5)
	if err != nil {
		log.Fatalf("register grpc service fail:%s\n", err.Error())
	}
	serList, err := serviceReg.Find("rpcServer")
	if err != nil {
		log.Fatalf("find service error %s\n", err.Error())
	}
	if len(serList) == 0 {
		log.Println("no service fund")
	} else {
		log.Println("service fund:")
		for _, ser := range serList {
			fmt.Println(ser.ServiceID())
		}
	}

	serviceReg.Subscribe("rpcServer", func(services []regdis.Service) {
		fmt.Println("subscribe service changed,now service list is:")
		for _, ser := range services {
			fmt.Println(ser.ServiceID())
		}
	})
	time.Sleep(time.Minute * 3)
}
