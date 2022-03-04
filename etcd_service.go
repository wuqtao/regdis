package regdis

import (
	"context"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"sync/atomic"
	"time"
)

const ServiceNameAndIDSeparator = "/"

type etcdServiceReg struct {
	service      Service
	etcdClient   *clientv3.Client
	savePath     string
	lease        clientv3.Lease
	leaseId      clientv3.LeaseID
	isUnregister int32 //原子操作，用于判定是否已经手动取消注册
	timeOut      time.Duration
}

func newEtcdServiceReg(ser Service, client *clientv3.Client, savePath string, timeOut time.Duration) *etcdServiceReg {
	return &etcdServiceReg{
		service:    ser,
		etcdClient: client,
		savePath:   savePath,
		timeOut:    timeOut,
	}
}

func (es *etcdServiceReg) Register() error {
	lease := clientv3.NewLease(es.etcdClient)
	ctx, _ := context.WithTimeout(context.Background(), es.timeOut)
	leResp, err := lease.Grant(ctx, 10)
	if err != nil {
		return err
	}

	es.lease = lease
	es.leaseId = leResp.ID
	kv := clientv3.NewKV(es.etcdClient)

	key := fmt.Sprintf("%s%s%s%s", es.savePath, es.service.ServiceName(), ServiceNameAndIDSeparator, es.service.ServiceID())
	ctx, _ = context.WithTimeout(context.Background(), es.timeOut)
	_, err = kv.Put(ctx, key, es.service.String(), clientv3.WithLease(leResp.ID))
	if err != nil {
		return err
	}
	log.Printf("服务id:%s注册成功\n", es.service.ServiceID())
	return es.regLoop(leResp.ID)
}

func (es *etcdServiceReg) regLoop(leaseId clientv3.LeaseID) error {
	keepRespChan, err := es.lease.KeepAlive(context.TODO(), leaseId)
	if err != nil {
		return err
	}
	//自动续租，保证注册有效性
	go func() {
		for {
			select {
			case keepResp := <-keepRespChan:
				if keepResp == nil {
					//未主动停止注册的情况下，则重新注册
					if atomic.LoadInt32(&es.isUnregister) == 0 {
						log.Printf("服务id:%s租约失效，重新注册\n", es.service.ServiceID())
						err := es.Register()
						if err != nil {
							log.Printf("服务id:%s租约失效，重新注册服务失败:%s\n", es.service.ServiceID(), err.Error())
						}
					}
					return
				}
			}
		}
	}()
	return nil
}

func (es *etcdServiceReg) Deregister() error {
	//未取消过才可以取消
	if atomic.CompareAndSwapInt32(&es.isUnregister, 0, 1) {
		fmt.Printf("服务id:%s取消注册\n", es.service.ServiceID())
		ctx, _ := context.WithTimeout(context.Background(), es.timeOut)
		_, err := es.lease.Revoke(ctx, es.leaseId)
		return err
	}
	return errors.New("service already deregister , do not repeat")
}
