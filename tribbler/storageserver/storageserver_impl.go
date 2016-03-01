package storageserver

import (
	"container/list"
	"errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
	"time"
)

type clientWithLease struct {
	clientPort string
	lease      storagerpc.Lease
	startTime  time.Time
}

type storageServer struct {
	// TODO: implement this!
	valueMap     map[string]string
	valueListMap map[string]*list.List
	slavesList   []storagerpc.Node
	numNodes     int
	nodeID       uint32
	isMaster     bool
	slaveMap     map[string]storagerpc.Node
	cacheLockMap map[string]*sync.RWMutex
	// valueLockMap      map[string]*sync.RWMutex
	// valueListLockMap  map[string]*sync.RWMutex
	lock              *sync.Mutex
	libstoreMap       map[string]*rpc.Client       //libstore hostport to rpc.Client
	keyToLeaseInfoMap map[string][]clientWithLease // key to lease information
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	storageServer := &storageServer{
		valueMap:     make(map[string]string),
		valueListMap: make(map[string]*list.List),
		slavesList:   make([]storagerpc.Node, numNodes),
		numNodes:     numNodes,
		nodeID:       nodeID,
		slaveMap:     make(map[string]storagerpc.Node),
		cacheLockMap: make(map[string]*sync.RWMutex),
		// valueLockMap:      make(map[string]*sync.RWMutex),
		// valueListLockMap:  make(map[string]*sync.RWMutex),readWriteLock
		lock:              &sync.Mutex{},
		libstoreMap:       make(map[string]*rpc.Client),
		keyToLeaseInfoMap: make(map[string][]clientWithLease),

		//slaveMap
	}

	if masterServerHostPort == "" {
		storageServer.isMaster = true
	} else {
		storageServer.isMaster = false
	}

	connection, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(storageServer))
	if err != nil {
		return nil, errors.New("Cannot register storage server on port") //%s", strconv.Itoa(port)
	}

	rpc.HandleHTTP()
	go http.Serve(connection, nil)

	// register this storage server
	node := storagerpc.Node{
		HostPort: "localhost:" + strconv.Itoa(port),
		NodeID:   nodeID,
	}
	registerArgs := &storagerpc.RegisterArgs{
		ServerInfo: node,
	}
	registerReply := &storagerpc.RegisterReply{}

	// if this storage server is a master
	if storageServer.isMaster {
		// go storageServer.RegisterServer(registerArgs, registerReply)
		storageServer.RegisterServer(registerArgs, registerReply)
	} else {
		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, errors.New("Storage server cannot connect to master server")
		}
		cli.Call("StorageServer.RegisterServer", registerArgs, registerReply)
		for registerReply.Status != storagerpc.OK {
			time.Sleep(1000 * time.Millisecond)
			cli.Call("StorageServer.RegisterServer", registerArgs, registerReply)

		}
		storageServer.slavesList = registerReply.Servers
	}
	return storageServer, nil
}

type byHostPort []storagerpc.Node

func (b byHostPort) Len() int {
	return len(b)
}

func (b byHostPort) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byHostPort) Less(i, j int) bool {
	return b[i].NodeID < b[j].NodeID
}

// only master server will execute this method, so there is no race condition
func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	if ss.isMaster == false {
		// return errors.New("Cannot register server on a slave")
		return nil
	}

	// when a server call register, lock the slaveMap, add to the map
	// only master server has this function
	ss.lock.Lock()
	ss.slaveMap[args.ServerInfo.HostPort] = args.ServerInfo
	if len(ss.slaveMap) < ss.numNodes {
		reply.Servers = nil
		reply.Status = storagerpc.NotReady
	}
	if len(ss.slaveMap) == ss.numNodes {
		index := 0
		for _, value := range ss.slaveMap {
			ss.slavesList[index] = value
			index++
		}
		sort.Sort(byHostPort(ss.slavesList))
		reply.Servers = ss.slavesList
		reply.Status = storagerpc.OK

	}
	ss.lock.Unlock()
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {

	// only master server has this function
	ss.lock.Lock()
	if len(ss.slaveMap) < ss.numNodes {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	} else {
		reply.Servers = ss.slavesList
		reply.Status = storagerpc.OK
	}
	ss.lock.Unlock()
	return nil
}

// after all storage servers are ready, this method can be called
// so the slaveList will not change at this time, so don't need lock
func (ss *storageServer) CheckIsCorrectServer(key string) bool {
	hashValue := libstore.StoreHash(key)
	nodeID := ss.slavesList[0].NodeID
	for _, id := range ss.slavesList {
		if hashValue <= id.NodeID {
			nodeID = id.NodeID
			break
		}
	}
	if nodeID == ss.nodeID {
		return true
	}
	return false

}

func (ss *storageServer) CheckLeaseSatus(key string) {
	dur := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	<-time.After(time.Duration(dur) * time.Second)

	// after lease time, delete this lease in storage server
	ss.lock.Lock()
	delete(ss.keyToLeaseInfoMap, key)
	ss.lock.Unlock()
	return

}

func (ss *storageServer) CallRevokeLease(key string, port string, RevokeSignal chan bool) {
	// send RPC revokeLease to libstore
	cli := ss.libstoreMap[port]
	revokeArgs := &storagerpc.RevokeLeaseArgs{
		Key: key,
	}
	revokeReply := &storagerpc.RevokeLeaseReply{}
	revokeReply.Status = storagerpc.NotReady
	cli.Call("LeaseCallbacks.RevokeLease", revokeArgs, revokeReply)
	for revokeReply.Status != storagerpc.OK {
		cli.Call("LeaseCallbacks.RevokeLease", revokeArgs, revokeReply)
	}
	RevokeSignal <- true

}

func (ss *storageServer) RevokeLeaseFromEachLib(key string, leaseInfo clientWithLease, FinishChan chan bool) {
	RevokeSignal := make(chan bool)
	dur := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	port := leaseInfo.clientPort
	go ss.CallRevokeLease(key, port, RevokeSignal)
	select {
	case <-RevokeSignal:
		FinishChan <- true
	case <-time.After(time.Duration(dur) * time.Second):
		FinishChan <- true
	}
}

func (ss *storageServer) RevokeLease(key string) {
	// ss.cacheLockMap[key].Lock()
	// defer ss.cacheLockMap[key].Unlock()

	value := ss.keyToLeaseInfoMap[key]
	FinishChan := make(chan bool)
	count := 0
	finishCount := 0
	for _, leaseInfo := range value {
		count++
		go ss.RevokeLeaseFromEachLib(key, leaseInfo, FinishChan)
	}
	for {
		select {
		case <-FinishChan:
			finishCount++
			if finishCount == count {
				ss.lock.Lock()
				delete(ss.keyToLeaseInfoMap, key)
				ss.lock.Unlock()
				return
			}
		}
	}

}

func (ss *storageServer) CheckConnectionWithLibstore(HostPort string) {
	_, exist := ss.libstoreMap[HostPort]
	if !exist {
		conn, _ := rpc.DialHTTP("tcp", HostPort)
		ss.lock.Lock()
		ss.libstoreMap[HostPort] = conn
		ss.lock.Unlock()
	}

}

func (ss *storageServer) GetLockForKey(Key string) {
	ss.lock.Lock()
	_, lockExist := ss.cacheLockMap[Key]
	if !lockExist {
		ss.cacheLockMap[Key] = &sync.RWMutex{}
	}
	ss.lock.Unlock()
}

func (ss *storageServer) CreateLease(args *storagerpc.GetArgs) storagerpc.Lease {
	lease := storagerpc.Lease{
		Granted:      true,
		ValidSeconds: storagerpc.LeaseSeconds,
	}
	newClientWithLease := clientWithLease{
		clientPort: args.HostPort,
		lease:      lease,
		startTime:  time.Now(),
	}

	// get the lock for this key
	// if you want to modify, you must get the lock for this key
	ss.cacheLockMap[args.Key].Lock()

	// lock for modify/read the whole map
	LeaseSclice, ScliceExist := ss.keyToLeaseInfoMap[args.Key]

	if ScliceExist {
		LeaseSclice = append(LeaseSclice, newClientWithLease)

		// lock for modify the whole map
		ss.lock.Lock()
		ss.keyToLeaseInfoMap[args.Key] = LeaseSclice
		ss.lock.Unlock()
	} else {
		var slice []clientWithLease
		slice = append(slice, newClientWithLease)

		// lock for modify the whole map
		ss.lock.Lock()
		ss.keyToLeaseInfoMap[args.Key] = slice
		ss.lock.Unlock()
	}
	ss.cacheLockMap[args.Key].Unlock()
	return lease

}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	rightServer := ss.CheckIsCorrectServer(args.Key)
	if !rightServer {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.lock.Lock()
	_, ok := ss.valueMap[args.Key]
	ss.lock.Unlock()

	if ok == false {
		reply.Status = storagerpc.KeyNotFound
		return nil

	} else {
		if args.WantLease {
			// check if this libstore is in map
			ss.CheckConnectionWithLibstore(args.HostPort)

			// get the read wirte lock of this key
			ss.GetLockForKey(args.Key)

			// create the new lease to map
			lease := ss.CreateLease(args)

			reply.Lease = lease
			reply.Value = ss.valueMap[args.Key]
			reply.Status = storagerpc.OK

			go ss.CheckLeaseSatus(args.Key)

		} else {
			reply.Value = ss.valueMap[args.Key]
			reply.Status = storagerpc.OK
		}
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	// check if this the correct server

	// get the read wirte lock of this key
	ss.GetLockForKey(args.Key)

	ss.cacheLockMap[args.Key].Lock()
	// defer ss.cacheLockMap[args.Key].Unlock()
	rightServer := ss.CheckIsCorrectServer(args.Key)
	if !rightServer {
		reply.Status = storagerpc.WrongServer

		ss.cacheLockMap[args.Key].Unlock()
		return nil
	}

	// revoke lease if there is a lease
	ss.lock.Lock()
	_, keyExist := ss.keyToLeaseInfoMap[args.Key]
	ss.lock.Unlock()
	if keyExist {
		ss.RevokeLease(args.Key)
	}

	// delte this key
	ss.lock.Lock()
	_, ok := ss.valueMap[args.Key]
	if ok == false {
		reply.Status = storagerpc.KeyNotFound
	} else {
		delete(ss.valueMap, args.Key)
		reply.Status = storagerpc.OK
	}
	ss.lock.Unlock()

	ss.cacheLockMap[args.Key].Unlock()
	return nil
}

func (ss *storageServer) GetListValue(Key string) []string {
	ss.lock.Lock()
	valueList := ss.valueListMap[Key]
	ss.lock.Unlock()

	var replyValue []string
	for e := valueList.Front(); e != nil; e = e.Next() {
		item := e.Value.(string)
		replyValue = append(replyValue, item)
	}
	return replyValue

}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// check if this the correct server
	rightServer := ss.CheckIsCorrectServer(args.Key)
	if !rightServer {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.lock.Lock()
	_, ok := ss.valueListMap[args.Key]
	ss.lock.Unlock()

	if ok == false {
		reply.Status = storagerpc.KeyNotFound
	} else {

		if args.WantLease {
			// check if this libstore is in map
			ss.CheckConnectionWithLibstore(args.HostPort)

			// get the read wirte lock of this key
			ss.GetLockForKey(args.Key)

			// add the new lease to map
			lease := ss.CreateLease(args)

			reply.Value = ss.GetListValue(args.Key)
			reply.Lease = lease
			reply.Status = storagerpc.OK

			go ss.CheckLeaseSatus(args.Key)

		} else {
			reply.Value = ss.GetListValue(args.Key)
			reply.Status = storagerpc.OK

		}

	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// get the read wirte lock of this key
	ss.GetLockForKey(args.Key)

	ss.cacheLockMap[args.Key].Lock()
	// defer ss.cacheLockMap[args.Key].Unlock()
	// check if this the correct server
	rightServer := ss.CheckIsCorrectServer(args.Key)
	if !rightServer {
		reply.Status = storagerpc.WrongServer

		ss.cacheLockMap[args.Key].Unlock()
		return nil
	}

	// revoke lease if there is a lease
	ss.lock.Lock()
	_, keyExist := ss.keyToLeaseInfoMap[args.Key]
	ss.lock.Unlock()
	if keyExist {
		ss.RevokeLease(args.Key)
	}

	ss.lock.Lock()
	ss.valueMap[args.Key] = args.Value
	reply.Status = storagerpc.OK
	ss.lock.Unlock()

	ss.cacheLockMap[args.Key].Unlock()
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	// get the read wirte lock of this key
	ss.GetLockForKey(args.Key)

	ss.cacheLockMap[args.Key].Lock()
	// defer ss.cacheLockMap[args.Key].Unlock()
	// check if this the correct server
	rightServer := ss.CheckIsCorrectServer(args.Key)
	if !rightServer {
		reply.Status = storagerpc.WrongServer

		ss.cacheLockMap[args.Key].Unlock()
		return nil
	}

	// revoke lease if there is a lease
	ss.lock.Lock()
	_, keyExist := ss.keyToLeaseInfoMap[args.Key]
	ss.lock.Unlock()
	if keyExist {
		ss.RevokeLease(args.Key)
	}

	valueList, ok := ss.valueListMap[args.Key]
	// if find this key and it's list
	if ok {
		inList := false
		for e := valueList.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == args.Value {
				reply.Status = storagerpc.ItemExists
				inList = true
				break
			}
		}

		// if this value doesn't exist
		if !inList {
			valueList.PushBack(args.Value)
			reply.Status = storagerpc.OK
		}
	} else {
		valueList = list.New()
		valueList.PushBack(args.Value)
		reply.Status = storagerpc.OK
	}

	ss.lock.Lock()
	ss.valueListMap[args.Key] = valueList
	ss.lock.Unlock()

	ss.cacheLockMap[args.Key].Unlock()

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	// get the read wirte lock of this key
	ss.GetLockForKey(args.Key)

	ss.cacheLockMap[args.Key].Lock()
	// defer ss.cacheLockMap[args.Key].Unlock()
	// check if this the correct server
	rightServer := ss.CheckIsCorrectServer(args.Key)
	if !rightServer {
		reply.Status = storagerpc.WrongServer

		ss.cacheLockMap[args.Key].Unlock()
		return nil
	}

	// revoke lease if there is a lease
	ss.lock.Lock()
	_, keyExist := ss.keyToLeaseInfoMap[args.Key]
	ss.lock.Unlock()
	if keyExist {
		ss.RevokeLease(args.Key)
	}

	valueList, ok := ss.valueListMap[args.Key]

	// if this key is in map
	if ok {
		found := false
		removed := valueList.Front()
		for e := valueList.Front(); e != nil; e = e.Next() {
			if e.Value.(string) == args.Value {
				removed = e
				reply.Status = storagerpc.OK
				found = true
			}
		}

		if !found {
			reply.Status = storagerpc.ItemNotFound
		} else {
			valueList.Remove(removed)
			ss.lock.Lock()
			ss.valueListMap[args.Key] = valueList
			ss.lock.Unlock()
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	ss.cacheLockMap[args.Key].Unlock()
	return nil
}
