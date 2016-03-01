package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
)

type countTime struct {
	queryTime []time.Time
	lease     storagerpc.Lease
}

type libstore struct {
	// TODO: implement this!
	client             *rpc.Client
	WantLease          bool
	HostPort           string
	storageServerSlice []storagerpc.Node
	connectionMap      map[string]*rpc.Client
	cacheMap           map[string]interface{}
	cacheTimeMap       map[string]*countTime
	LeaseMode          LeaseMode
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}

	count := 0
	for count < 5 {
		cli.Call("StorageServer.GetServers", args, reply)
		if reply.Status == storagerpc.NotReady {
			count++
			time.Sleep(1000 * time.Millisecond)
		}
		if reply.Status == storagerpc.OK {
			break
		}
	}

	libstore := &libstore{
		client:             cli,
		WantLease:          false,
		HostPort:           myHostPort,
		storageServerSlice: reply.Servers,
		connectionMap:      make(map[string]*rpc.Client),
		cacheMap:           make(map[string]interface{}),
		cacheTimeMap:       make(map[string]*countTime),
		LeaseMode:          mode,
	}
	libstore.connectionMap[masterServerHostPort] = cli
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))

	// go libstore.checkCacheLease()
	return libstore, nil
}

func (ls *libstore) checkLeaseTimeout(lease storagerpc.Lease, key string) {
	<-time.After(time.Duration(lease.ValidSeconds) * time.Second)
	delete(ls.cacheMap, key)
	// delete(ls.cacheTimeMap, key)
	return

}

func (ls *libstore) checkNeedLease(key string) bool {
	_, ok := ls.cacheTimeMap[key]
	if !ok {
		countTime := &countTime{
			queryTime: make([]time.Time, 0),
		}
		ls.cacheTimeMap[key] = countTime
	}
	countTime, _ := ls.cacheTimeMap[key]
	list := append(countTime.queryTime, time.Now())
	countTime.queryTime = list
	ls.cacheTimeMap[key] = countTime

	if len(countTime.queryTime) < storagerpc.QueryCacheThresh {
		return false
	} else {
		startTime := countTime.queryTime[len(countTime.queryTime)-storagerpc.QueryCacheThresh]
		list = countTime.queryTime[len(countTime.queryTime)-storagerpc.QueryCacheThresh:]
		countTime.queryTime = list
		ls.cacheTimeMap[key] = countTime
		if time.Since(startTime).Seconds() < storagerpc.QueryCacheSeconds {
			return true
		}
	}
	return false

}

func (ls *libstore) FindStorageServer(key string) string {
	hashValue := StoreHash(key)
	hostPort := ""
	for _, server := range ls.storageServerSlice {
		if server.NodeID >= hashValue {
			hostPort = server.HostPort
			break
		}
	}
	if hostPort == "" {
		hostPort = ls.storageServerSlice[0].HostPort
	}
	return hostPort
}

func (ls *libstore) GetConnection(key string) *rpc.Client {
	serverHostPort := ls.FindStorageServer(key)
	conn, ok := ls.connectionMap[serverHostPort]
	if !ok {
		conn, _ = rpc.DialHTTP("tcp", serverHostPort)
		ls.connectionMap[serverHostPort] = conn
	}
	return conn
}

func (ls *libstore) Get(key string) (string, error) {
	// search in the libstore cache
	val, ok := ls.cacheMap[key]
	if ok {
		if ls.cacheTimeMap[key].lease.Granted {
			str, _ := val.(string)
			return str, nil
		}
	}

	want := ls.checkNeedLease(key)
	if ls.LeaseMode == Always {
		want = true
	}
	if ls.LeaseMode == Never {
		want = false
	}

	port := ls.HostPort
	getArgs := &storagerpc.GetArgs{
		Key:       key,
		WantLease: want,
		HostPort:  port, // The Libstore's callback host:port.
	}
	getReply := &storagerpc.GetReply{}

	// find which storage server should go
	conn := ls.GetConnection(key)

	conn.Call("StorageServer.Get", getArgs, getReply)
	if getReply.Status != storagerpc.OK {
		return "", errors.New("Error in libstore Get")
	}

	if getArgs.WantLease && getReply.Lease.Granted {
		ls.cacheMap[key] = getReply.Value
		ls.cacheTimeMap[key].lease = getReply.Lease
		go ls.checkLeaseTimeout(getReply.Lease, key)

	}

	return getReply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	putArgs := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	putReply := &storagerpc.PutReply{}

	// get the connection with the storage server which this request should go
	conn := ls.GetConnection(key)

	conn.Call("StorageServer.Put", putArgs, putReply)
	if putReply.Status != storagerpc.OK {
		return errors.New("Error in libstore Put")
	}
	return nil

}

func (ls *libstore) Delete(key string) error {
	deleteArgs := &storagerpc.DeleteArgs{
		Key: key,
	}
	deleteReply := &storagerpc.DeleteReply{}

	// get the connection with the storage server which this request should go
	conn := ls.GetConnection(key)

	conn.Call("StorageServer.Delete", deleteArgs, deleteReply)
	if deleteReply.Status != storagerpc.OK {
		return errors.New("Error in libstore Delete")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	val, ok := ls.cacheMap[key]
	if ok {
		if ls.cacheTimeMap[key].lease.Granted {
			str, _ := val.([]string)
			return str, nil
		}

	}

	want := ls.checkNeedLease(key)
	if ls.LeaseMode == Always {
		want = true
	}
	if ls.LeaseMode == Never {
		want = false
	}
	port := ls.HostPort
	getArgs := &storagerpc.GetArgs{
		Key:       key,
		WantLease: want,
		HostPort:  port,
	}
	getListReply := &storagerpc.GetListReply{}

	// get the connection with the storage server which this request should go
	conn := ls.GetConnection(key)

	conn.Call("StorageServer.GetList", getArgs, getListReply)

	if getListReply.Status != storagerpc.OK {
		return nil, errors.New("Error in libstore GetList")
	}

	if getArgs.WantLease && getListReply.Lease.Granted {
		ls.cacheMap[key] = getListReply.Value
		ls.cacheTimeMap[key].lease = getListReply.Lease
		go ls.checkLeaseTimeout(getListReply.Lease, key)
	}
	return getListReply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	putArgs := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	putReply := &storagerpc.PutReply{}

	// get the connection with the storage server which this request should go
	conn := ls.GetConnection(key)

	conn.Call("StorageServer.RemoveFromList", putArgs, putReply)

	if putReply.Status != storagerpc.OK {
		return errors.New("Error in libstore RemoveFromList")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	putArgs := &storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}
	putReply := &storagerpc.PutReply{}

	// get the connection with the storage server which this request should go
	conn := ls.GetConnection(key)

	conn.Call("StorageServer.AppendToList", putArgs, putReply)

	if putReply.Status != storagerpc.OK {
		return errors.New("Error in libstore AppendToList")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	delete(ls.cacheMap, args.Key)
	reply.Status = storagerpc.OK
	return nil
}
