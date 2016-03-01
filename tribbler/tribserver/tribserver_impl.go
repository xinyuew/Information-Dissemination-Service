package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"
)

type tribServer struct {
	// TODO: implement this!
	libstore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	tribServer := &tribServer{
		libstore: libstore,
	}
	connection, err := net.Listen("tcp", myHostPort)

	// register this tribble server
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, errors.New("Cannot register tribble server")
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(connection, nil)
	return tribServer, nil

}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	UserID := args.UserID
	key := util.FormatUserKey(UserID)
	_, err := ts.libstore.Get(key)

	// if err is nil, find this userID
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	} else {
		ts.libstore.Put(util.FormatUserKey(UserID), UserID)
		reply.Status = tribrpc.OK
		return nil
	}

}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	UserID := args.UserID
	TargetUserID := args.TargetUserID
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))

	// this user doesn't exit
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		// return errors.New("This user doesn't exist when add subscription")
		return nil
	}

	// target user doesn't exist
	_, err = ts.libstore.Get(util.FormatUserKey(TargetUserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		// return errors.New("The target user doesn't exist when add subscription")
		return nil
	}

	// key is fmt.Sprintf("%s:sublist", userID)
	key := util.FormatSubListKey(UserID)
	err = ts.libstore.AppendToList(key, TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	UserID := args.UserID
	TargetUserID := args.TargetUserID
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))

	// this user doesn't exit
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		fmt.Println("Tribe server RemoveSubscription error 1")
		return nil
	}

	// target user doesn't exist
	_, err = ts.libstore.Get(util.FormatUserKey(TargetUserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		fmt.Println("Tribe server RemoveSubscription error 2")
		return nil
	}

	// key is fmt.Sprintf("%s:sublist", userID)
	key := util.FormatSubListKey(UserID)

	// check if this item is in list when deleting
	err = ts.libstore.RemoveFromList(key, TargetUserID)
	if err != nil {
		fmt.Println("Tribe server RemoveSubscription error 3")
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	fmt.Println("Tribe server RemoveSubscription 33333")
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	UserID := args.UserID
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))

	// this user doesn't exit
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// key is fmt.Sprintf("%s:sublist", userID)
	key := util.FormatSubListKey(UserID)
	list, err := ts.libstore.GetList(key)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	reply.Status = tribrpc.OK
	reply.UserIDs = list
	return nil

}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	UserID := args.UserID
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	postTime := time.Now().UTC().UnixNano()
	key := util.FormatPostKey(UserID, postTime)
	_, exist := ts.libstore.Get(key)

	for exist == nil {
		postTime := time.Now().UTC().UnixNano()
		key = util.FormatPostKey(UserID, postTime)
		_, exist = ts.libstore.Get(key)
	}
	tribble := &tribrpc.Tribble{
		UserID:   UserID,
		Posted:   time.Now(),
		Contents: args.Contents,
	}
	tribbleMarshall, err := json.Marshal(tribble)

	// put this tribble id and tribble into map
	value := string(tribbleMarshall[:])
	err = ts.libstore.Put(key, value)
	if err != nil {
		return nil
	}

	// put this tribble id to user's tribbles list, variable "key" is tribble id
	key_list := util.FormatTribListKey(UserID)
	err = ts.libstore.AppendToList(key_list, key)
	if err != nil {
		reply.Status = tribrpc.Exists
		// return errors.New("Put tribble into list error")
		return nil
	}

	reply.Status = tribrpc.OK

	// postKey is the id of this tribble
	reply.PostKey = key
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	UserID := args.UserID
	PostKey := args.PostKey
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// check if this post exist
	_, err = ts.libstore.Get(PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}

	// delete from post map
	ts.libstore.Delete(PostKey)

	// delete from user's post list
	key := util.FormatTribListKey(UserID)
	err = ts.libstore.RemoveFromList(key, PostKey)
	if err != nil {
		return nil
	}
	reply.Status = tribrpc.OK
	return nil

}

type byTime []tribrpc.Tribble

func (b byTime) Len() int {
	return len(b)
}

func (b byTime) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byTime) Less(i, j int) bool {
	return b[i].Posted.After(b[j].Posted)
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	UserID := args.UserID
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	key := util.FormatTribListKey(UserID)
	tribIdList, err := ts.libstore.GetList(key)

	tribList := make([]tribrpc.Tribble, len(tribIdList))

	for i := 0; i < len(tribIdList); i++ {
		value, _ := ts.libstore.Get(tribIdList[i])
		var tribble tribrpc.Tribble
		json.Unmarshal([]byte(value), &tribble)
		tribList[i] = tribble
	}

	// sort the list
	sort.Sort(byTime(tribList))

	var topList []tribrpc.Tribble
	length := 100
	if len(tribList) < 100 {
		length = len(tribList)
	}
	for i := 0; i < length; i++ {
		topList = append(topList, tribList[i])
	}
	reply.Tribbles = topList
	reply.Status = tribrpc.OK
	return nil

}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	UserID := args.UserID
	_, err := ts.libstore.Get(util.FormatUserKey(UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	subUserIdListKey := util.FormatSubListKey(UserID)
	subUserIdList, err := ts.libstore.GetList(subUserIdListKey)

	var tribIdListAll []tribrpc.Tribble

	// loop over all follows users
	for i := 0; i < len(subUserIdList); i++ {
		list := ts.GetTribbleByUser(subUserIdList[i])
		tribIdListAll = append(tribIdListAll, list...)
	}

	sort.Sort(byTime(tribIdListAll))

	var topList []tribrpc.Tribble

	length := 100
	if len(tribIdListAll) < 100 {
		length = len(tribIdListAll)
	}
	for i := 0; i < length; i++ {
		topList = append(topList, tribIdListAll[i])
	}

	reply.Tribbles = topList
	reply.Status = tribrpc.OK
	return nil

}

func (ts *tribServer) GetTribbleByUser(userID string) []tribrpc.Tribble {
	key := util.FormatTribListKey(userID)
	tribIdList, _ := ts.libstore.GetList(key)
	tribList := make([]tribrpc.Tribble, len(tribIdList))

	for i := 0; i < len(tribIdList); i++ {
		value, _ := ts.libstore.Get(tribIdList[i])
		var tribble tribrpc.Tribble
		json.Unmarshal([]byte(value), &tribble)
		tribList[i] = tribble
	}
	return tribList
}
