package etcdreg

import (
	"context"
	"errors"
	"fmt"
	"github.com/wuqtao/regdis"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"strings"
	"sync"
	"time"
)

type EtcdRegistrationDiscoveryImp struct {
	callBackMap     map[string]regdis.ServiceCallback //存储服务变化订阅的callback,key为serviceName，针对一个服务只能有一个订阅回调
	callBackMapLock *sync.RWMutex

	fundServiceMap     map[string]map[string]regdis.Service //存储已经发现的服务，第一层key为service name，第二层key为service ID
	fundServiceMapLock *sync.RWMutex

	registerServiceMap map[string]*EtcdServiceReg //存储已经注册的服务,key为serviceName:serviceId
	registerMapLock    *sync.RWMutex

	savePath   string           //etcd存储路径
	etcdClient *clientv3.Client //etcdreg client v3
}

func NewEtcdRegistrationDiscoveryImp(endpoints []string, userName, password, savePath string) (regdis.RegistrationAndDiscovery, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
		Username:    userName,
		Password:    password,
	})

	if err != nil {
		return nil, err
	}

	etcdReg := &EtcdRegistrationDiscoveryImp{
		callBackMap:        make(map[string]regdis.ServiceCallback),
		callBackMapLock:    &sync.RWMutex{},
		registerServiceMap: make(map[string]*EtcdServiceReg),
		registerMapLock:    &sync.RWMutex{},
		fundServiceMap:     make(map[string]map[string]regdis.Service),
		fundServiceMapLock: &sync.RWMutex{},
		etcdClient:         client,
		savePath:           fmt.Sprintf("/%s/", strings.Trim(savePath, "/")), //标准存储路径为/path/
	}

	go etcdReg.run()
	return etcdReg, nil
}

func (e *EtcdRegistrationDiscoveryImp) run() {
	//启东时先从etcd读取一次数据
	kv := clientv3.NewKV(e.etcdClient)
	resp, err := kv.Get(context.TODO(), e.savePath, clientv3.WithPrefix())
	if err != nil {
		log.Printf("query etcdreg error %s\n", err.Error())
		return
	}

	//记录读取数据时的最大revision
	maxRevision := resp.Header.Revision

	e.fundServiceMapLock.Lock()
	//将读取到的数据存储到发现列表
	for _, kv := range resp.Kvs {
		currSer, err := regdis.ParseServiceFromStr(string(kv.Value))
		if err != nil {
			log.Printf("run parse.ParseServiceFromStr error %s--%v\n", err.Error(), kv.Value)
			continue
		}

		if serMap, ok := e.fundServiceMap[currSer.ServiceName()]; ok {
			serMap[currSer.ServiceID()] = currSer
		} else {
			serMap = map[string]regdis.Service{}
			serMap[currSer.ServiceID()] = currSer
			e.fundServiceMap[currSer.ServiceName()] = serMap
		}
	}
	e.fundServiceMapLock.Unlock()
	//从最前面查找到的数据的max revision + 1 开始watch，即watch上次查找后的对应目录的一切变动
	watchChan := e.etcdClient.Watch(context.TODO(), e.savePath, clientv3.WithPrefix(), clientv3.WithRev(maxRevision+1))
	for {
		select {
		case ser := <-watchChan:
			//为了方便去重，直接使用map，int值无实际用途
			changeServiceNameList := map[string]int{}
			e.fundServiceMapLock.Lock()
			for _, event := range ser.Events {
				//解析变动的可以，获取对应的serviceName、serviceID
				//无论是put事件还是delete事件都有key，delete事件没有value
				serName, serID, err := getServiceNameAndId(e.savePath, string(event.Kv.Key))
				if err != nil {
					log.Printf("getServiceNameAndId error %s\n", err.Error())
					continue
				}

				//记录当前变动的服务名称
				changeServiceNameList[serName] = 1
				serMap, ok := e.fundServiceMap[serName]
				if !ok {
					serMap = make(map[string]regdis.Service)
				}

				//从字符串解析出service对象
				switch event.Type {
				case clientv3.EventTypePut:
					log.Printf("新增服务id:%s\n", serID)
					currSer, err := regdis.ParseServiceFromStr(string(event.Kv.Value))
					if err != nil {
						log.Printf("parse.ParseServiceFromStr error %s--%v\n", err.Error(), event.Kv.Value)
						continue
					}
					//无需关心是新增还是修改，直接将新的ser对象存起来即可
					serMap[serID] = currSer
				case clientv3.EventTypeDelete: //删除事件，kv在prekv中
					log.Printf("删除服务id:%s\n", serID)
					delete(serMap, serID)
				}
				//处理后统一赋值
				e.fundServiceMap[serName] = serMap
			}

			//检查订阅事件中的服务
			e.callBackMapLock.RLock()
			for k, v := range e.callBackMap {
				//有变动则调用相应的函数
				if _, ok := changeServiceNameList[k]; ok {
					newSers := []regdis.Service{}
					serMap, ok := e.fundServiceMap[k]
					if ok {
						for _, ser := range serMap {
							newSers = append(newSers, ser)
						}
					}
					//此处传输的是专门构造的slice，并发安全，直接新启用协程加快调度速度，减少锁占用时间
					if v != nil {
						go v(newSers)
					}
				}

			}
			e.callBackMapLock.RUnlock()
			e.fundServiceMapLock.Unlock()
		}
	}
}

//向注册中心注册服务
func (e *EtcdRegistrationDiscoveryImp) Register(ser regdis.Service) error {
	e.registerMapLock.RLock()
	if _, ok := e.registerServiceMap[getRegisterMapKey(ser)]; ok {
		e.registerMapLock.RUnlock()
		return errors.New("same name and same id service already register do not repeat")
	}
	e.registerMapLock.RUnlock()

	etcdService := NewEtcdServiceReg(ser, e.etcdClient, e.savePath)
	err := etcdService.Register()
	if err != nil {
		return err
	}

	//sync.Map非并发安全的，所以加锁，然后把注册的服务存入
	e.registerMapLock.Lock()
	defer e.registerMapLock.Unlock()
	e.registerServiceMap[getRegisterMapKey(ser)] = etcdService

	return nil
}

//从注册中心取消注册
func (e *EtcdRegistrationDiscoveryImp) Deregister(ser regdis.Service) error {
	e.registerMapLock.Lock()
	defer e.registerMapLock.Unlock()
	serKey := getRegisterMapKey(ser)
	if curSer, ok := e.registerServiceMap[serKey]; ok {
		err := curSer.Deregister()
		if err != nil {
			log.Println("Deregister error " + err.Error())
			return err
		}
		delete(e.registerServiceMap, serKey)
	}
	return nil
}

//根据服务名查找服务列表
func (e *EtcdRegistrationDiscoveryImp) Find(serviceName string) ([]regdis.Service, error) {
	//因为已经启动了所有的service的监控，所以并不需要再次从etcd中查询，直接取本地存储值即可
	e.fundServiceMapLock.RLock()
	defer e.fundServiceMapLock.RUnlock()
	if serMap, ok := e.fundServiceMap[serviceName]; ok {
		serList := []regdis.Service{}
		for _, v := range serMap {
			serList = append(serList, v)
		}
		return serList, nil
	}
	return nil, errors.New("there is no service for given name")
}

//供外部使用，订阅某个服务的变化情况，一个服务只能有一个订阅回调
func (e *EtcdRegistrationDiscoveryImp) Subscribe(serviceName string, callback regdis.ServiceCallback) error {
	e.callBackMapLock.Lock()
	defer e.callBackMapLock.Unlock()
	if _, ok := e.callBackMap[serviceName]; ok {
		return errors.New("can not repeat subscribe the same service by name")
	}
	e.callBackMap[serviceName] = callback
	return nil
}

//取消针对服务名的订阅
func (e *EtcdRegistrationDiscoveryImp) Unsubscribe(serviceName string) error {
	e.callBackMapLock.Lock()
	defer e.callBackMapLock.Unlock()
	delete(e.callBackMap, serviceName)
	return nil
}

func getRegisterMapKey(ser regdis.Service) string {
	return fmt.Sprintf("%s:%s", ser.ServiceName(), ser.ServiceID())
}

//通过给定的etcd Key获取serviceName,serviceId
func getServiceNameAndId(savePath, key string) (serviceName, serviceId string, err error) {
	str := strings.Replace(key, savePath, "", 1)
	//str := string(key[len(savePath):])
	if str != "" && strings.Contains(str, ServiceNameAndIDSeparator) {
		strArr := strings.Split(str, ServiceNameAndIDSeparator)
		if len(strArr) == 2 {
			return strArr[0], strArr[1], nil
		}
	}
	return "", "", errors.New("error service value")
}
